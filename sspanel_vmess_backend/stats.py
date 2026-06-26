from __future__ import annotations

import logging

from .config import XrayConfig
from .models import TrafficDelta, User
from .state import JsonState
from .xray import run_stats_query

logger = logging.getLogger(__name__)


class StatsCollector:
    def __init__(self, config: XrayConfig) -> None:
        self.config = config
        self.state = JsonState(config.stats_cursor_path)
        self.cursor = self.state.load()

    def collect(self, users: list[User]) -> list[TrafficDelta]:
        raw_stats = run_stats_query(self.config)
        users_by_email = {user.vmess_email: user for user in users}
        current = normalize_user_stats(raw_stats, users_by_email)

        if not self.cursor.get("initialized"):
            self.cursor = {"initialized": True, "stats": current}
            self.state.data = self.cursor
            self.state.save()
            logger.info("stats cursor initialized; first cycle traffic is not billed")
            return []

        previous: dict[str, dict[str, int]] = self.cursor.get("stats", {})
        deltas: list[TrafficDelta] = []
        for email, values in current.items():
            user = users_by_email[email]
            old_values = previous.get(email, {})
            upload = max(0, values.get("uplink", 0) - int(old_values.get("uplink", 0)))
            download = max(0, values.get("downlink", 0) - int(old_values.get("downlink", 0)))
            if upload or download:
                deltas.append(TrafficDelta(user_id=user.id, upload=upload, download=download))

        self.cursor = {"initialized": True, "stats": current}
        self.state.data = self.cursor
        return deltas

    def commit(self) -> None:
        self.state.save()


def normalize_user_stats(raw_stats: dict[str, int], users_by_email: dict[str, User]) -> dict[str, dict[str, int]]:
    current: dict[str, dict[str, int]] = {
        email: {"uplink": 0, "downlink": 0}
        for email in users_by_email
    }
    for name, value in raw_stats.items():
        for email in users_by_email:
            if f"user>>>{email}>>>traffic>>>uplink" == name:
                current[email]["uplink"] = value
            elif f"user>>>{email}>>>traffic>>>downlink" == name:
                current[email]["downlink"] = value
    return current

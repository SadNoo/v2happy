from __future__ import annotations

import logging
import os
import time

from .accesslog import AccessLogCollector
from .config import AppConfig
from .db import Database
from .models import AliveIp, TrafficDelta
from .stats import StatsCollector
from .v2ray import v2_array
from .xray import XrayConfigManager

logger = logging.getLogger(__name__)


class BackendService:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.database = Database(config.database)
        self.xray = XrayConfigManager(config.xray, dry_run=config.runtime.dry_run)
        self.stats = StatsCollector(config.xray)
        self.access_log = AccessLogCollector(
            config.xray.access_log_path,
            config.xray.access_cursor_path,
        )
        self.started_at = time.time()
        self.last_good_users_count = 0

    def run(self) -> None:
        while True:
            self.run_once()
            if self.config.runtime.run_once:
                break
            time.sleep(self.config.node.sync_interval_seconds)

    def run_once(self) -> None:
        node = self.database.fetch_node(self.config.node.id)
        if node is None:
            logger.warning("node %s unavailable or bandwidth-limited; keeping current runtime", self.config.node.id)
            return
        if node.sort not in (11, 12):
            logger.warning("node %s sort=%s is not V2Ray/VMess; keeping current runtime", node.node_id, node.sort)
            return

        endpoint = v2_array(node.server)
        users = self.database.fetch_users(node)
        if not users and self.last_good_users_count and self.config.node.protect_on_empty_users:
            logger.warning("empty user list after previous non-empty state; keeping current runtime")
            return

        self.xray.apply(endpoint, users)
        self.last_good_users_count = len(users)

        if self.config.runtime.dry_run:
            logger.info(
                "dry-run cycle: users=%s port=%s network=%s",
                len(users),
                endpoint.port,
                endpoint.net,
            )
            return

        traffic = safe_collect_traffic(self.stats, users)
        alive_ips = self.access_log.collect(users)
        online_user_count = count_online_users(traffic)
        uptime = int(time.time() - self.started_at)
        load = read_load_average()

        self.database.write_cycle(
            node=node,
            traffic=traffic,
            alive_ips=alive_ips,
            online_user_count=online_user_count,
            uptime=uptime,
            load=load,
        )
        self.stats.commit()
        self.access_log.commit()
        logger.info(
            "cycle completed: users=%s traffic=%s alive_ips=%s online=%s port=%s",
            len(users),
            len(traffic),
            len(alive_ips),
            online_user_count,
            endpoint.port,
        )


def safe_collect_traffic(stats: StatsCollector, users: list) -> list[TrafficDelta]:
    try:
        return stats.collect(users)
    except Exception:
        logger.exception("failed to collect xray stats; traffic cursor not advanced")
        return []


def count_online_users(traffic: list[TrafficDelta]) -> int:
    return sum(1 for item in traffic if item.upload or item.download)


def read_load_average() -> str:
    try:
        return " ".join(f"{item:.2f}" for item in os.getloadavg())
    except OSError:
        return "0.00 0.00 0.00"

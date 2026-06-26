from __future__ import annotations

import ipaddress
from pathlib import Path
import re
from typing import Optional, Tuple

from .models import AliveIp, User
from .state import JsonState

EMAIL_RE = re.compile(r"sspanel-user-(?P<user_id>\d+)")
IP_RE = re.compile(
    r"(?P<ip>(?:\d{1,3}\.){3}\d{1,3}|[0-9a-fA-F:]{2,})"
)


class AccessLogCollector:
    def __init__(self, log_path: str, cursor_path: str) -> None:
        self.log_path = Path(log_path)
        self.state = JsonState(cursor_path)
        self.cursor = self.state.load()

    def collect(self, users: list[User]) -> list[AliveIp]:
        if not self.log_path.exists():
            return []
        user_ids = {user.id for user in users}
        offset = int(self.cursor.get("offset", 0))
        file_size = self.log_path.stat().st_size
        if offset > file_size:
            offset = 0

        alive: set[tuple[int, str]] = set()
        with self.log_path.open("r", encoding="utf-8", errors="ignore") as handle:
            handle.seek(offset)
            for line in handle:
                parsed = parse_access_line(line)
                if parsed is None:
                    continue
                user_id, ip = parsed
                if user_id in user_ids:
                    alive.add((user_id, ip))
            self.cursor["offset"] = handle.tell()

        self.state.data = self.cursor
        return [AliveIp(user_id=user_id, ip=ip) for user_id, ip in sorted(alive)]

    def commit(self) -> None:
        self.state.save()


def parse_access_line(line: str) -> Optional[Tuple[int, str]]:
    email_match = EMAIL_RE.search(line)
    if email_match is None:
        return None

    user_id = int(email_match.group("user_id"))
    for match in IP_RE.finditer(line):
        candidate = match.group("ip").strip("[]")
        try:
            ip = ipaddress.ip_address(candidate)
        except ValueError:
            continue
        if ip.is_loopback or ip.is_unspecified:
            continue
        return user_id, str(ip)
    return None

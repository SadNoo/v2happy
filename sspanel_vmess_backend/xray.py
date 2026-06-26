from __future__ import annotations

import hashlib
import json
import logging
import os
from pathlib import Path
import subprocess
import time
from typing import Any, Optional, Sequence, TYPE_CHECKING

from .models import User, V2RayEndpoint
from .v2ray import build_xray_config

if TYPE_CHECKING:
    from .config import XrayConfig

logger = logging.getLogger(__name__)


class XrayConfigManager:
    def __init__(self, config: XrayConfig, dry_run: bool = False) -> None:
        self.config = config
        self.dry_run = dry_run

    def apply(self, endpoint: V2RayEndpoint, users: list[User]) -> bool:
        xray_config = build_xray_config(
            endpoint=endpoint,
            users=users,
            listen=self.config.listen,
            api_address=self.config.api_address,
            access_log_path=self.config.access_log_path,
            error_log_path=self.config.error_log_path,
        )
        rendered = json.dumps(xray_config, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        target = Path(self.config.config_path)
        old_digest = file_digest(target)
        new_digest = hashlib.sha256(rendered.encode("utf-8")).hexdigest()

        if old_digest == new_digest:
            logger.info("xray config unchanged")
            return False

        logger.info("xray config changed: port=%s users=%s", endpoint.port, len(users))
        if self.dry_run:
            logger.info("dry-run enabled; not writing xray config")
            return True

        target.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = target.with_suffix(target.suffix + ".tmp")
        tmp_path.write_text(rendered, encoding="utf-8")
        os.replace(tmp_path, target)
        self.reload()
        return True

    def reload(self) -> None:
        command = self.config.reload_command or self.config.restart_command
        if not command:
            logger.warning("xray config written but no reload_command/restart_command configured")
            return
        logger.info("running xray reload command: %s", " ".join(command))
        subprocess.run(command, check=True)


def file_digest(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def render_command(command: Sequence[str], *, binary: str, api_address: str, pattern: str) -> list[str]:
    return [
        part.format(binary=binary, api_address=api_address, pattern=pattern)
        for part in command
    ]


def default_stats_command(binary: str, api_address: str, pattern: str) -> list[str]:
    return [
        binary,
        "api",
        "statsquery",
        "--server",
        api_address,
        "-pattern",
        pattern,
    ]


def run_stats_query(config: XrayConfig, pattern: str = "user>>>") -> dict[str, int]:
    if config.stats_command:
        command = render_command(
            config.stats_command,
            binary=config.binary,
            api_address=config.api_address,
            pattern=pattern,
        )
    else:
        command = default_stats_command(config.binary, config.api_address, pattern)

    last_error: Optional[subprocess.CalledProcessError] = None
    for attempt in range(1, 6):
        logger.debug("running xray stats command: %s", " ".join(command))
        completed = subprocess.run(command, capture_output=True, text=True)
        if completed.returncode == 0:
            return parse_stats_output(completed.stdout)

        last_error = subprocess.CalledProcessError(
            completed.returncode,
            command,
            output=completed.stdout,
            stderr=completed.stderr,
        )
        logger.warning(
            "xray stats command failed attempt %s/5 exit=%s stdout=%r stderr=%r",
            attempt,
            completed.returncode,
            completed.stdout.strip(),
            completed.stderr.strip(),
        )
        time.sleep(attempt)

    assert last_error is not None
    raise last_error


def parse_stats_output(output: str) -> dict[str, int]:
    output = output.strip()
    if not output:
        return {}

    try:
        data = json.loads(output)
        return parse_stats_json(data)
    except json.JSONDecodeError:
        return parse_stats_text(output)


def parse_stats_json(data: Any) -> dict[str, int]:
    stats: dict[str, int] = {}
    if isinstance(data, dict):
        entries = data.get("stat") or data.get("stats") or data.get("results") or []
    elif isinstance(data, list):
        entries = data
    else:
        entries = []

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        value = entry.get("value")
        if name is None or value is None:
            continue
        stats[str(name)] = int(value)
    return stats


def parse_stats_text(output: str) -> dict[str, int]:
    stats: dict[str, int] = {}
    for line in output.splitlines():
        if ">>>" not in line:
            continue
        normalized = line.replace(":", " ").replace("=", " ")
        parts = normalized.split()
        for index, part in enumerate(parts):
            if ">>>" not in part:
                continue
            for candidate in parts[index + 1 :]:
                if candidate.lstrip("-").isdigit():
                    stats[part] = int(candidate)
                    break
            break
    return stats

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised on Python 3.9/3.10
    import tomli as tomllib


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    charset: str = "utf8"
    ssl_enable: bool = False
    ssl_ca: str = ""
    ssl_cert: str = ""
    ssl_key: str = ""


@dataclass(frozen=True)
class NodeConfig:
    id: int
    sync_interval_seconds: int = 60
    protect_on_empty_users: bool = True


@dataclass(frozen=True)
class XrayConfig:
    binary: str
    config_path: str
    listen: str = "0.0.0.0"
    api_address: str = "127.0.0.1:10085"
    access_log_path: str = "/var/log/xray/access.log"
    error_log_path: str = "/var/log/xray/error.log"
    stats_cursor_path: str = "/var/lib/sspanel-vmess/stats.json"
    access_cursor_path: str = "/var/lib/sspanel-vmess/access-log.json"
    reload_command: Optional[list[str]] = None
    stats_command: Optional[list[str]] = None
    restart_command: Optional[list[str]] = None


@dataclass(frozen=True)
class RuntimeConfig:
    dry_run: bool = False
    run_once: bool = False
    state_dir: str = "/var/lib/sspanel-vmess"


@dataclass(frozen=True)
class AppConfig:
    database: DatabaseConfig
    node: NodeConfig
    xray: XrayConfig
    runtime: RuntimeConfig


def _string_list(value: Optional[object]) -> Optional[list[str]]:
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError("command values must be TOML arrays")
    return [str(item) for item in value]


def load_config(path: Union[str, Path]) -> AppConfig:
    config_path = Path(path)
    data = tomllib.loads(config_path.read_text(encoding="utf-8"))

    database = data.get("database", {})
    node = data.get("node", {})
    xray = data.get("xray", {})
    runtime = data.get("runtime", {})

    return AppConfig(
        database=DatabaseConfig(
            host=str(database["host"]),
            port=int(database.get("port", 3306)),
            user=str(database["user"]),
            password=str(database["password"]),
            database=str(database["database"]),
            charset=str(database.get("charset", "utf8")),
            ssl_enable=bool(database.get("ssl_enable", False)),
            ssl_ca=str(database.get("ssl_ca", "")),
            ssl_cert=str(database.get("ssl_cert", "")),
            ssl_key=str(database.get("ssl_key", "")),
        ),
        node=NodeConfig(
            id=int(node["id"]),
            sync_interval_seconds=int(node.get("sync_interval_seconds", 60)),
            protect_on_empty_users=bool(node.get("protect_on_empty_users", True)),
        ),
        xray=XrayConfig(
            binary=str(xray.get("binary", "xray")),
            config_path=str(xray["config_path"]),
            listen=str(xray.get("listen", "0.0.0.0")),
            api_address=str(xray.get("api_address", "127.0.0.1:10085")),
            access_log_path=str(xray.get("access_log_path", "/var/log/xray/access.log")),
            error_log_path=str(xray.get("error_log_path", "/var/log/xray/error.log")),
            stats_cursor_path=str(xray.get("stats_cursor_path", "/var/lib/sspanel-vmess/stats.json")),
            access_cursor_path=str(xray.get("access_cursor_path", "/var/lib/sspanel-vmess/access-log.json")),
            reload_command=_string_list(xray.get("reload_command")),
            stats_command=_string_list(xray.get("stats_command")),
            restart_command=_string_list(xray.get("restart_command")),
        ),
        runtime=RuntimeConfig(
            dry_run=bool(runtime.get("dry_run", False)),
            run_once=bool(runtime.get("run_once", False)),
            state_dir=str(runtime.get("state_dir", "/var/lib/sspanel-vmess")),
        ),
    )

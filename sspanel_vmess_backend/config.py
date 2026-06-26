from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    user: str
    password: str
    database: str


@dataclass(frozen=True)
class NodeConfig:
    id: int
    check_rate: int = 60
    speedtest_check_rate: int = 0


@dataclass(frozen=True)
class PanelConfig:
    url: str = "https://google.com"
    key: str = ""
    type: int = 0
    down_with_panel: int = 0
    use_mysql: int = 1
    mu_regex: str = "%5m%id.%suffix"
    mu_suffix: str = "microsoft.com"


@dataclass(frozen=True)
class V2RayConfig:
    config_path: str = "/etc/v2ray/config.json"
    loglevel: str = "info"
    restart_command: Optional[list[str]] = None


@dataclass(frozen=True)
class PluginOptions:
    cf_key: str = "bbbbbbbbbbbbbbbbbb"
    cf_email: str = "v2ray@v2ray.com"
    proxy_tcp: int = 0
    ali_key: str = "sdfsdfsdfljlbjkljlkjsdfoiwje"
    ali_secret: str = "jlsdflanljkljlfdsaklkjflsa"
    cache_duration_sec: int = 60
    html_path: str = ""


@dataclass(frozen=True)
class RuntimeConfig:
    dry_run: bool = False
    print_config: bool = False


@dataclass(frozen=True)
class AppConfig:
    database: DatabaseConfig
    node: NodeConfig
    panel: PanelConfig
    v2ray: V2RayConfig
    plugin: PluginOptions
    runtime: RuntimeConfig


def _string_list(value: Optional[object]) -> Optional[list[str]]:
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError("command values must be TOML arrays")
    return [str(item) for item in value]


def load_config(path: Union[str, Path]) -> AppConfig:
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover - exercised on Python 3.9/3.10
        import tomli as tomllib

    config_path = Path(path)
    data = tomllib.loads(config_path.read_text(encoding="utf-8"))

    database = data.get("database", {})
    node = data.get("node", {})
    panel = data.get("panel", {})
    v2ray = data.get("v2ray", {})
    plugin = data.get("plugin", {})
    runtime = data.get("runtime", {})

    return AppConfig(
        database=DatabaseConfig(
            host=str(database["host"]),
            port=int(database.get("port", 3306)),
            user=str(database["user"]),
            password=str(database["password"]),
            database=str(database["database"]),
        ),
        node=NodeConfig(
            id=int(node["id"]),
            check_rate=int(node.get("check_rate", node.get("sync_interval_seconds", 60))),
            speedtest_check_rate=int(node.get("speedtest_check_rate", 0)),
        ),
        panel=PanelConfig(
            url=str(panel.get("url", "https://google.com")),
            key=str(panel.get("key", "")),
            type=int(panel.get("type", 0)),
            down_with_panel=int(panel.get("down_with_panel", 0)),
            use_mysql=int(panel.get("use_mysql", 1)),
            mu_regex=str(panel.get("mu_regex", "%5m%id.%suffix")),
            mu_suffix=str(panel.get("mu_suffix", "microsoft.com")),
        ),
        v2ray=V2RayConfig(
            config_path=str(v2ray.get("config_path", "/etc/v2ray/config.json")),
            loglevel=str(v2ray.get("loglevel", "info")),
            restart_command=_string_list(v2ray.get("restart_command")),
        ),
        plugin=PluginOptions(
            cf_key=str(plugin.get("cf_key", "bbbbbbbbbbbbbbbbbb")),
            cf_email=str(plugin.get("cf_email", "v2ray@v2ray.com")),
            proxy_tcp=int(plugin.get("proxy_tcp", 0)),
            ali_key=str(plugin.get("ali_key", "sdfsdfsdfljlbjkljlkjsdfoiwje")),
            ali_secret=str(plugin.get("ali_secret", "jlsdflanljkljlfdsaklkjflsa")),
            cache_duration_sec=int(plugin.get("cache_duration_sec", 60)),
            html_path=str(plugin.get("html_path", "")),
        ),
        runtime=RuntimeConfig(
            dry_run=bool(runtime.get("dry_run", False)),
            print_config=bool(runtime.get("print_config", False)),
        ),
    )

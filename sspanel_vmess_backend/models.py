from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class NodeInfo:
    node_id: int
    node_group: int
    node_class: int
    node_speedlimit: float
    traffic_rate: float
    mu_only: int
    sort: int
    server: str
    node_bandwidth: int
    node_bandwidth_limit: int


@dataclass(frozen=True)
class User:
    id: int
    email: str
    port: int
    passwd: str
    forbidden_ip: str
    forbidden_port: str
    disconnect_ip: str
    node_speedlimit: float
    u: int
    d: int
    transfer_enable: int
    enable: int
    uuid: str
    vmess_email: str


@dataclass(frozen=True)
class V2RayEndpoint:
    add: str
    port: int
    aid: int
    net: str = "tcp"
    type: str = "none"
    host: str = ""
    path: str = ""
    tls: str = ""
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TrafficDelta:
    user_id: int
    upload: int
    download: int

    @property
    def total(self) -> int:
        return self.upload + self.download


@dataclass(frozen=True)
class AliveIp:
    user_id: int
    ip: str


def to_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    if value == "":
        return default
    if isinstance(value, Decimal):
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)

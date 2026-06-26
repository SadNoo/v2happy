from __future__ import annotations

from typing import Any
from uuid import NAMESPACE_DNS, uuid3

from .models import User, V2RayEndpoint


def parse_args(origin: str) -> dict[str, str]:
    result: dict[str, str] = {}
    if not origin:
        return result
    for arg in origin.split("|"):
        if not arg:
            continue
        split_point = arg.find("=")
        if split_point == -1:
            result[arg] = ""
            continue
        result[arg[:split_point]] = arg[split_point + 1 :]
    return result


def v2_array(node_server: str) -> V2RayEndpoint:
    server = node_server.split(";")
    if len(server) < 3:
        raise ValueError("ss_node.server for V2Ray must contain at least address;port;alterId")

    item: dict[str, Any] = {
        "host": "",
        "path": "",
        "tls": "",
        "add": server[0],
        "port": 443 if server[1] in ("", "0") else int(server[1]),
        "aid": int(server[2]),
        "net": "tcp",
        "type": "none",
    }

    if len(server) >= 4 and server[3] != "":
        item["net"] = server[3]
        if item["net"] == "ws":
            item["path"] = "/"
        elif item["net"] == "tls":
            item["tls"] = "tls"

    if len(server) >= 5 and server[4] != "":
        if item["net"] in ("kcp", "http"):
            item["type"] = server[4]
        elif server[4] == "ws":
            item["net"] = "ws"

    if len(server) >= 6:
        item.update(parse_args(server[5]))
        if "server" in item:
            item["add"] = item.pop("server")
        if "outside_port" in item:
            item["port"] = int(item.pop("outside_port"))

    return V2RayEndpoint(
        add=str(item["add"]),
        port=int(item["port"]),
        aid=int(item["aid"]),
        net=str(item.get("net", "tcp")),
        type=str(item.get("type", "none")),
        host=str(item.get("host", "")),
        path=str(item.get("path", "")),
        tls=str(item.get("tls", "")),
        raw=item,
    )


def user_uuid(user_id: int, passwd: str) -> str:
    return str(uuid3(NAMESPACE_DNS, f"{user_id}|{passwd}"))


def vmess_email(user_id: int) -> str:
    return f"sspanel-user-{user_id}"


def build_stream_settings(endpoint: V2RayEndpoint) -> dict[str, Any]:
    settings: dict[str, Any] = {"network": endpoint.net}

    if endpoint.tls == "tls":
        settings["security"] = "tls"
        settings["tlsSettings"] = {
            "serverName": endpoint.host or endpoint.add,
        }
    else:
        settings["security"] = "none"

    if endpoint.net == "ws":
        ws_settings: dict[str, Any] = {"path": endpoint.path or "/"}
        if endpoint.host:
            ws_settings["headers"] = {"Host": endpoint.host}
        settings["wsSettings"] = ws_settings
    elif endpoint.net == "kcp":
        settings["kcpSettings"] = {"header": {"type": endpoint.type or "none"}}
    elif endpoint.net == "http":
        http_settings: dict[str, Any] = {"path": endpoint.path or "/"}
        if endpoint.host:
            http_settings["host"] = [host.strip() for host in endpoint.host.split(",") if host.strip()]
        settings["httpSettings"] = http_settings
    elif endpoint.net == "tcp" and endpoint.type and endpoint.type != "none":
        settings["tcpSettings"] = {"header": {"type": endpoint.type}}

    return settings


def build_xray_config(
    endpoint: V2RayEndpoint,
    users: list[User],
    listen: str,
    api_address: str,
    access_log_path: str,
    error_log_path: str,
) -> dict[str, Any]:
    clients = [
        {
            "id": user.uuid,
            "alterId": endpoint.aid,
            "email": user.vmess_email,
        }
        for user in sorted(users, key=lambda item: item.id)
    ]

    api_host, api_port = split_host_port(api_address)

    routing_rules = build_user_block_rules(users)
    routing_rules.append(
        {
            "type": "field",
            "inboundTag": ["api"],
            "outboundTag": "api",
        }
    )

    return {
        "log": {
            "access": access_log_path,
            "error": error_log_path,
            "loglevel": "warning",
        },
        "stats": {},
        "api": {
            "tag": "api",
            "services": ["StatsService"],
        },
        "policy": {
            "levels": {
                "0": {
                    "statsUserUplink": True,
                    "statsUserDownlink": True,
                }
            },
            "system": {
                "statsInboundUplink": True,
                "statsInboundDownlink": True,
                "statsOutboundUplink": True,
                "statsOutboundDownlink": True,
            },
        },
        "inbounds": [
            {
                "tag": "sspanel-vmess",
                "listen": listen,
                "port": endpoint.port,
                "protocol": "vmess",
                "settings": {
                    "clients": clients,
                },
                "streamSettings": build_stream_settings(endpoint),
            },
            {
                "tag": "api",
                "listen": api_host,
                "port": api_port,
                "protocol": "dokodemo-door",
                "settings": {"address": api_host},
            },
        ],
        "outbounds": [
            {"tag": "direct", "protocol": "freedom"},
            {"tag": "blocked", "protocol": "blackhole"},
        ],
        "routing": {
            "rules": routing_rules,
        },
    }


def split_host_port(value: str) -> tuple[str, int]:
    host, sep, port = value.rpartition(":")
    if not sep:
        raise ValueError("api_address must be host:port")
    return host, int(port)


def build_user_block_rules(users: list[User]) -> list[dict[str, Any]]:
    rules: list[dict[str, Any]] = []
    for user in users:
        disconnect_sources = split_csv(user.disconnect_ip)
        if disconnect_sources:
            rules.append(
                {
                    "type": "field",
                    "user": [user.vmess_email],
                    "source": disconnect_sources,
                    "outboundTag": "blocked",
                }
            )

        forbidden_ips = split_csv(user.forbidden_ip)
        if forbidden_ips:
            rules.append(
                {
                    "type": "field",
                    "user": [user.vmess_email],
                    "ip": forbidden_ips,
                    "outboundTag": "blocked",
                }
            )

        forbidden_ports = normalize_port_rule(user.forbidden_port)
        if forbidden_ports:
            rules.append(
                {
                    "type": "field",
                    "user": [user.vmess_email],
                    "port": forbidden_ports,
                    "outboundTag": "blocked",
                }
            )
    return rules


def split_csv(value: str) -> list[str]:
    return [
        item.strip()
        for item in value.replace("\n", ",").split(",")
        if item.strip()
    ]


def normalize_port_rule(value: str) -> str:
    ports = split_csv(value)
    return ",".join(item.replace(":", "-") for item in ports)

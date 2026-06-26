from __future__ import annotations

import json
from pathlib import Path
import subprocess
from typing import Any

from .config import AppConfig


def build_v2ray_plugin_config(config: AppConfig) -> dict[str, Any]:
    return {
        "dns": {
            "servers": [
                "1.1.1.1",
                {
                    "address": "localhost",
                    "port": 53,
                    "domains": media_dns_domains(),
                },
            ]
        },
        "inbounds": [],
        "log": {
            "loglevel": config.v2ray.loglevel,
        },
        "outbounds": [
            {
                "sendThrough": "0.0.0.0",
                "protocol": "freedom",
                "settings": {"domainStrategy": "UseIPv4"},
            },
            {
                "protocol": "blackhole",
                "settings": {"response": {"type": "http"}},
                "tag": "blocked",
            },
            {
                "protocol": "blackhole",
                "settings": {"response": {"type": "none"}},
                "tag": "blockedNodeUserLimited",
            },
            {
                "protocol": "blackhole",
                "settings": {"response": {"type": "http"}},
                "tag": "blockedrule",
            },
            {
                "protocol": "blackhole",
                "settings": {"response": {"type": "http"}},
                "tag": "blockedip",
            },
            {
                "protocol": "blackhole",
                "settings": {},
                "tag": "block",
            },
        ],
        "policy": {
            "levels": {
                "0": {
                    "connIdle": 300,
                    "downlinkOnly": 0,
                    "handshake": 4,
                    "statsUserDownlink": True,
                    "statsUserUplink": True,
                    "uplinkOnly": 0,
                }
            },
            "system": {
                "statsInboundDownlink": False,
                "statsInboundUplink": False,
            },
        },
        "reverse": {},
        "routing": {
            "rules": [
                {
                    "outboundTag": "blocked",
                    "protocol": ["bittorrent"],
                    "type": "field",
                },
                {
                    "outboundTag": "blockedNodeUserLimited",
                    "user": ["userlimitedBlocked@v2v3.xxx"],
                    "type": "field",
                },
            ],
            "domainStrategy": "AsIs",
        },
        "stats": {},
        "sspanel": {
            "nodeid": config.node.id,
            "checkRate": config.node.check_rate,
            "SpeedTestCheckRate": config.node.speedtest_check_rate,
            "panelUrl": config.panel.url,
            "panelKey": config.panel.key,
            "downWithPanel": config.panel.down_with_panel,
            "mu_regex": config.panel.mu_regex,
            "mu_suffix": config.panel.mu_suffix,
            "mysql": {
                "host": config.database.host,
                "port": config.database.port,
                "user": config.database.user,
                "password": config.database.password,
                "dbname": config.database.database,
            },
            "paneltype": config.panel.type,
            "usemysql": config.panel.use_mysql,
            "cf_key": config.plugin.cf_key,
            "cf_email": config.plugin.cf_email,
            "proxy_tcp": config.plugin.proxy_tcp,
            "ali_key": config.plugin.ali_key,
            "ali_secret": config.plugin.ali_secret,
            "cache_duration_sec": config.plugin.cache_duration_sec,
            "html_path": config.plugin.html_path,
        },
    }


def write_config(config: AppConfig, payload: dict[str, Any]) -> None:
    rendered = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    if config.runtime.print_config:
        print(rendered, end="")
    if config.runtime.dry_run:
        return

    path = Path(config.v2ray.config_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(rendered, encoding="utf-8")
    tmp_path.replace(path)

    if config.v2ray.restart_command:
        subprocess.run(config.v2ray.restart_command, check=True)


def media_dns_domains() -> list[str]:
    return [
        "domain:akadns.net",
        "domain:akam.net",
        "domain:akamai.com",
        "domain:akamai.net",
        "domain:akamaiedge.net",
        "domain:akamaihd.net",
        "domain:akamaistream.net",
        "domain:akamaitech.net",
        "domain:akamaitechnologies.com",
        "domain:akamaitechnologies.fr",
        "domain:akamaized.net",
        "domain:edgekey.net",
        "domain:edgesuite.net",
        "domain:srip.net",
        "domain:footprint.net",
        "domain:level3.net",
        "domain:llnwd.net",
        "domain:edgecastcdn.net",
        "domain:cloudfront.net",
        "domain:netflix.com",
        "domain:netflix.net",
        "domain:nflximg.net",
        "domain:nflxvideo.net",
        "domain:nflxso.net",
        "domain:nflxext.com",
        "domain:hulu.com",
        "domain:huluim.com",
        "domain:hbonow.com",
        "domain:hbogo.com",
        "domain:hbo.com",
        "domain:amazon.com",
        "domain:amazon.co.uk",
        "domain:amazonvideo.com",
        "domain:crackle.com",
        "domain:pandora.com",
        "domain:vudu.com",
        "domain:blinkbox.com",
        "domain:abc.com",
        "domain:fox.com",
        "domain:theplatform.com",
        "domain:nbc.com",
        "domain:nbcuni.com",
        "domain:ip2location.com",
        "domain:pbs.org",
        "domain:warnerbros.com",
        "domain:southpark.cc.com",
        "domain:cbs.com",
        "domain:brightcove.com",
        "domain:cwtv.com",
        "domain:spike.com",
        "domain:go.com",
        "domain:mtv.com",
        "domain:mtvnservices.com",
        "domain:playstation.net",
        "domain:uplynk.com",
        "domain:maxmind.com",
        "domain:disney.com",
        "domain:disneyjunior.com",
        "domain:xboxlive.com",
        "domain:lovefilm.com",
        "domain:turner.com",
        "domain:amctv.com",
        "domain:sho.com",
        "domain:mog.com",
        "domain:wdtvlive.com",
        "domain:beinsportsconnect.tv",
        "domain:beinsportsconnect.net",
        "domain:fig.bbc.co.uk",
        "domain:open.live.bbc.co.uk",
        "domain:sa.bbc.co.uk",
        "domain:www.bbc.co.uk",
        "domain:crunchyroll.com",
        "domain:ifconfig.co",
        "domain:omtrdc.net",
        "domain:sling.com",
        "domain:movetv.com",
    ]

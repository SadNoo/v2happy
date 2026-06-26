from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .config import load_config
from .plugin_config import build_v2ray_plugin_config, write_config


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate custom V2Ray 4.32.1 SSPanel plugin config"
    )
    parser.add_argument(
        "-c",
        "--config",
        default="config.toml",
        help="Path to config TOML file",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Console log level",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    config = load_config(Path(args.config))
    payload = build_v2ray_plugin_config(config)
    write_config(config, payload)


if __name__ == "__main__":
    main()

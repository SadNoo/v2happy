from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .config import load_config
from .service import BackendService


def main() -> None:
    parser = argparse.ArgumentParser(description="SSPanel-Uim VMess single-port backend")
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

    config_path = Path(args.config)
    config = load_config(config_path)
    BackendService(config).run()


if __name__ == "__main__":
    main()

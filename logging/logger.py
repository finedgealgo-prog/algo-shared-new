"""
logger.py
─────────
Structured logging setup for algo-2_0. Always-on (unlike the old
shared/features/app_logger.py, which defaults LOGGING_ENABLED=False after a
prior disk-usage incident) — algo-2_0 uses rotation + level filtering instead
of an on/off switch to control volume, per the master-doc's "no silent
failures" / observability requirements.
"""

from __future__ import annotations

import logging
import logging.handlers
import os

from shared.config.settings import get_settings

_CONFIGURED = False


def configure_logging() -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return
    settings = get_settings()
    os.makedirs(settings.log_dir, exist_ok=True)

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )

    root = logging.getLogger()
    root.setLevel(settings.log_level)

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    root.addHandler(console)

    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=os.path.join(settings.log_dir, "algo-trade-2_0.log"),
        when="midnight",
        backupCount=30,
        utc=True,
    )
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    configure_logging()
    return logging.getLogger(name)

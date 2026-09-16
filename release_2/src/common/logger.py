"""Structured logging configuration for Databricks and Python runtime."""

import logging
import sys
from typing import Optional, Set

_REGISTERED_SECRETS: Set[str] = set()


def register_secret_to_mask(secret_value: Optional[str]) -> None:
    """Register a sensitive token/string to be masked in logs.
    
    Args:
        secret_value: String value that must not appear in log records.
    """
    if secret_value and len(secret_value.strip()) > 3:
        _REGISTERED_SECRETS.add(secret_value.strip())


def mask_secret(message: str) -> str:
    """Mask known sensitive secrets from message text."""
    masked = message
    for secret in _REGISTERED_SECRETS:
        if secret in masked:
            masked = masked.replace(secret, "[REDACTED_SECRET]")
    return masked


class SecretMaskingFilter(logging.Filter):
    """Logging filter that scrubs sensitive strings from emitted records."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = mask_secret(record.msg)
        if record.args:
            if isinstance(record.args, dict):
                record.args = {
                    k: mask_secret(v) if isinstance(v, str) else v
                    for k, v in record.args.items()
                }
            elif isinstance(record.args, tuple):
                record.args = tuple(
                    mask_secret(a) if isinstance(a, str) else a
                    for a in record.args
                )
        return True


def get_logger(name: str = "valo_analytics", level: int = logging.INFO) -> logging.Logger:
    """Creates or returns a pre-configured logger with formatting and secret masking.
    
    Args:
        name: Name of the logger (module name recommended).
        level: Logging level (default INFO).
        
    Returns:
        logging.Logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        handler.addFilter(SecretMaskingFilter())
        logger.addHandler(handler)

    # Ensure masking filter exists on all handlers
    for h in logger.handlers:
        if not any(isinstance(f, SecretMaskingFilter) for f in h.filters):
            h.addFilter(SecretMaskingFilter())

    return logger

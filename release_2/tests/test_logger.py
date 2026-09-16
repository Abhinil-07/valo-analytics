"""Unit tests for logger and secret masking."""

import logging
from src.common.logger import get_logger, register_secret_to_mask, mask_secret


def test_secret_masking():
    secret = "super_secret_valorant_api_key_xyz"
    register_secret_to_mask(secret)

    log_msg = f"Connecting with Authorization: {secret} to endpoint"
    masked = mask_secret(log_msg)

    assert secret not in masked
    assert "[REDACTED_SECRET]" in masked


def test_logger_emits_masked_record(capsys):
    logger = get_logger("test_secret_masker")
    secret = "another_secret_token_12345"
    register_secret_to_mask(secret)

    logger.info("Secret value: %s", secret)
    captured = capsys.readouterr().out

    assert secret not in captured
    assert "[REDACTED_SECRET]" in captured

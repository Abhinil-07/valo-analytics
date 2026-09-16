"""Unit tests for VolumeWriter."""

from datetime import date
import json
import os
import pytest
from configs.settings import IngestionConfig
from src.landing.validator import ValidatedMatch
from src.landing.volume_writer import VolumeWriter, VolumeWriterError


def test_volume_writer_path_generation(tmp_path):
    config = IngestionConfig(
        catalog="test_cat",
        schema="test_sch",
        volume="test_vol",
        base_volume_path_override=str(tmp_path)
    )
    writer = VolumeWriter(config)
    match = ValidatedMatch(
        match_id="ab4a85fb-8f9e-4298-b22e-b2a3648cb58b",
        match_date=date(2024, 1, 16),
        raw_payload={"id": "1"}
    )
    path = writer.get_target_path(match)
    expected_suffix = os.path.join("matches", "2024", "01", "16", "ab4a85fb-8f9e-4298-b22e-b2a3648cb58b.json").replace("\\", "/")
    assert path.endswith(expected_suffix)


def test_volume_writer_write_and_verify(tmp_path):
    config = IngestionConfig(base_volume_path_override=str(tmp_path))
    writer = VolumeWriter(config)

    raw_data = {"test_key": "test_value", "nested": {"num": 42}}
    match = ValidatedMatch(
        match_id="ab4a85fb-8f9e-4298-b22e-b2a3648cb58b",
        match_date=date(2024, 1, 16),
        raw_payload=raw_data
    )

    written_path = writer.write_match_json(match)
    assert os.path.exists(written_path)
    assert writer.file_exists(written_path) is True

    # Read back and verify content matches exactly
    with open(written_path, "r", encoding="utf-8") as f:
        loaded = json.load(f)
    assert loaded == raw_data

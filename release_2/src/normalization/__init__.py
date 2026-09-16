"""Normalization package for converting raw match payloads to Canonical Landing objects."""

from release_2.src.normalization.normalizer import (
    detect_source_format,
    unwrap_matches,
    normalize_match,
    validate_canonical_match,
    NormalizationError,
)

__all__ = [
    "detect_source_format",
    "unwrap_matches",
    "normalize_match",
    "validate_canonical_match",
    "NormalizationError",
]

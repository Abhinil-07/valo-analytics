"""Configuration package for Valorant Analytics."""
try:
    from release_2.configs.settings import IngestionConfig
except ImportError:
    from configs.settings import IngestionConfig

__all__ = ["IngestionConfig"]

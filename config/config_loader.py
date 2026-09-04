"""
Enterprise Rare Blood Type Availability Analytics
Config Loader
=====================================================

Every script that needs a business rule (rare blood groups, component
types, shortage-scoring weights, etc.) loads it through this module rather
than re-declaring its own copy. This is the "one source of truth" required
by the project upgrade brief (Section 12).

Usage:
    from config.config_loader import get_project_config, get_thresholds

    project = get_project_config()
    thresholds = get_thresholds()
    rare_groups = project["blood_groups"]["rare"]
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml

CONFIG_DIR = Path(__file__).resolve().parent
PROJECT_CONFIG_PATH = CONFIG_DIR / "project.yaml"
THRESHOLDS_CONFIG_PATH = CONFIG_DIR / "thresholds.yaml"


def _load_yaml(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Required config file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@lru_cache(maxsize=1)
def get_project_config() -> dict:
    """Blood groups, component types, donation statuses, transaction types."""
    return _load_yaml(PROJECT_CONFIG_PATH)


@lru_cache(maxsize=1)
def get_thresholds() -> dict:
    """Availability thresholds, shortage-scoring weights/bands, recommended actions."""
    return _load_yaml(THRESHOLDS_CONFIG_PATH)


# Convenience accessors used throughout the project
def get_valid_blood_groups() -> list[str]:
    return get_project_config()["blood_groups"]["valid"]


def get_rare_blood_groups() -> list[str]:
    return get_project_config()["blood_groups"]["rare"]


def get_valid_component_types() -> list[str]:
    return get_project_config()["component_types"]["valid"]


def get_component_shelf_life(component_type: str) -> int:
    return get_project_config()["component_types"]["shelf_life_days"][component_type]


def get_random_seed() -> int:
    return get_project_config()["random_seed"]

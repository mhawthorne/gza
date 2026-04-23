"""Parity tests for discoverable configuration key registry."""

from dataclasses import fields
from pathlib import Path

from gza.config import Config
from gza.config_schema import (
    CONFIG_KEY_REGISTRY,
    NON_CONFIG_ROOT_KEYS,
    RUNTIME_ONLY_CONFIG_FIELDS,
)


def test_config_registry_covers_all_config_fields() -> None:
    """All user-configurable Config dataclass fields should be discoverable in the key registry."""
    config_fields = {f.name for f in fields(Config)}
    user_fields = config_fields - set(RUNTIME_ONLY_CONFIG_FIELDS)
    registry_roots = {spec.key.split(".", 1)[0] for spec in CONFIG_KEY_REGISTRY}

    missing = user_fields - registry_roots
    assert not missing, f"Missing config roots in registry: {sorted(missing)}"


def test_config_registry_has_no_stale_keys() -> None:
    """Registry roots should match Config fields (plus explicit non-dataclass compatibility keys)."""
    config_fields = {f.name for f in fields(Config)}
    allowed_roots = (config_fields - set(RUNTIME_ONLY_CONFIG_FIELDS)) | set(NON_CONFIG_ROOT_KEYS)
    registry_roots = {spec.key.split(".", 1)[0] for spec in CONFIG_KEY_REGISTRY}

    stale = registry_roots - allowed_roots
    assert not stale, f"Stale/unknown config roots in registry: {sorted(stale)}"


def test_configuration_doc_mentions_all_registered_keys() -> None:
    """Operator docs should include every discoverable key listed by the registry."""
    docs_text = (Path(__file__).resolve().parents[1] / "docs" / "configuration.md").read_text()
    missing = [spec.key for spec in CONFIG_KEY_REGISTRY if spec.key not in docs_text]
    assert not missing, f"Missing config keys in docs/configuration.md: {missing}"


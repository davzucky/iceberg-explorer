"""Tests for configuration system."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from iceberg_explorer.config import (
    CatalogConfig,
    CatalogType,
    DuckDBConfig,
    OTelConfig,
    QueryConfig,
    ServerConfig,
    Settings,
    get_settings,
    load_settings,
    reset_settings,
)

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    """Clean environment and reset settings before/after each test."""
    env_vars_to_remove = [key for key in os.environ if key.startswith("ICEBERG_EXPLORER_")]
    for key in env_vars_to_remove:
        monkeypatch.delenv(key, raising=False)
    reset_settings()
    yield
    reset_settings()


class TestCatalogConfig:
    """Tests for CatalogConfig."""

    def test_defaults(self) -> None:
        config = CatalogConfig()
        assert config.type == CatalogType.REST
        assert config.uri == "http://localhost:8181"
        assert config.warehouse == ""
        assert config.name == "default"

    def test_local_catalog_type(self) -> None:
        config = CatalogConfig(type=CatalogType.LOCAL, warehouse="/data/warehouse")
        assert config.type == CatalogType.LOCAL
        assert config.warehouse == "/data/warehouse"


class TestQueryConfig:
    """Tests for QueryConfig."""

    def test_defaults(self) -> None:
        config = QueryConfig()
        assert config.max_rows == 10000
        assert config.default_timeout == 300
        assert config.min_timeout == 10
        assert config.max_timeout == 3600

    def test_custom_values(self) -> None:
        config = QueryConfig(max_rows=5000, default_timeout=60)
        assert config.max_rows == 5000
        assert config.default_timeout == 60

    def test_timeout_validation(self) -> None:
        with pytest.raises(ValueError):
            QueryConfig(default_timeout=5)  # Below min
        with pytest.raises(ValueError):
            QueryConfig(default_timeout=4000)  # Above max


class TestDuckDBConfig:
    """Tests for DuckDBConfig."""

    def test_defaults(self) -> None:
        config = DuckDBConfig()
        assert config.memory_limit == "4GB"
        assert config.threads == 4

    def test_custom_values(self) -> None:
        config = DuckDBConfig(memory_limit="8GB", threads=8)
        assert config.memory_limit == "8GB"
        assert config.threads == 8

    def test_threads_validation(self) -> None:
        with pytest.raises(ValueError):
            DuckDBConfig(threads=0)


class TestServerConfig:
    """Tests for ServerConfig."""

    def test_defaults(self) -> None:
        config = ServerConfig()
        assert config.host == "0.0.0.0"
        assert config.port == 8080

    def test_custom_values(self) -> None:
        config = ServerConfig(host="127.0.0.1", port=3000)
        assert config.host == "127.0.0.1"
        assert config.port == 3000


class TestOTelConfig:
    """Tests for OTelConfig."""

    def test_defaults(self) -> None:
        config = OTelConfig()
        assert config.enabled is False
        assert config.endpoint == "http://localhost:4317"
        assert config.service_name == "iceberg-explorer"

    def test_enabled(self) -> None:
        config = OTelConfig(enabled=True, endpoint="http://otel:4317")
        assert config.enabled is True
        assert config.endpoint == "http://otel:4317"


class TestSettings:
    """Tests for root Settings."""

    def test_defaults(self) -> None:
        settings = Settings()
        assert settings.catalog.type == CatalogType.REST
        assert settings.query.max_rows == 10000
        assert settings.duckdb.memory_limit == "4GB"
        assert settings.server.port == 8080
        assert settings.otel.enabled is False

    def test_load_from_json_file(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.json"
        config_data = {
            "catalog": {"type": "local", "warehouse": "/data/warehouse"},
            "query": {"max_rows": 5000},
            "duckdb": {"memory_limit": "8GB", "threads": 8},
        }
        config_file.write_text(json.dumps(config_data))

        os.environ["ICEBERG_EXPLORER_CONFIG"] = str(config_file)
        settings = Settings()

        assert settings.catalog.type == CatalogType.LOCAL
        assert settings.catalog.warehouse == "/data/warehouse"
        assert settings.query.max_rows == 5000
        assert settings.duckdb.memory_limit == "8GB"
        assert settings.duckdb.threads == 8

    def test_config_file_not_found(self, tmp_path: Path) -> None:
        os.environ["ICEBERG_EXPLORER_CONFIG"] = str(tmp_path / "nonexistent.json")
        with pytest.raises(ValueError, match="Config file not found"):
            Settings()

    def test_env_var_overrides(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ICEBERG_EXPLORER_QUERY__MAX_ROWS", "2000")
        monkeypatch.setenv("ICEBERG_EXPLORER_DUCKDB__THREADS", "16")

        settings = Settings()
        assert settings.query.max_rows == 2000
        assert settings.duckdb.threads == 16

    def test_json_file_with_env_overrides(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config_file = tmp_path / "config.json"
        config_data = {"query": {"max_rows": 5000}, "duckdb": {"threads": 4}}
        config_file.write_text(json.dumps(config_data))

        monkeypatch.setenv("ICEBERG_EXPLORER_CONFIG", str(config_file))
        monkeypatch.setenv("ICEBERG_EXPLORER_QUERY__MAX_ROWS", "3000")

        settings = Settings()
        assert settings.query.max_rows == 3000  # Env var wins
        assert settings.duckdb.threads == 4  # From file


class TestGetSettings:
    """Tests for get_settings function."""

    def test_caching(self) -> None:
        settings1 = get_settings()
        settings2 = get_settings()
        assert settings1 is settings2

    def test_reset_clears_cache(self) -> None:
        settings1 = get_settings()
        reset_settings()
        settings2 = get_settings()
        assert settings1 is not settings2


class TestLoadSettings:
    """Tests for load_settings function."""

    def test_load_from_path(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.json"
        config_data = {"catalog": {"type": "local", "warehouse": "/warehouse"}}
        config_file.write_text(json.dumps(config_data))

        settings = load_settings(config_file)
        assert settings.catalog.type == CatalogType.LOCAL
        assert settings.catalog.warehouse == "/warehouse"

    def test_load_with_string_path(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.json"
        config_data = {"server": {"port": 9000}}
        config_file.write_text(json.dumps(config_data))

        settings = load_settings(str(config_file))
        assert settings.server.port == 9000

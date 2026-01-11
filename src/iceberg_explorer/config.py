"""Configuration system for Iceberg Explorer.

Loads configuration from:
1. JSON file specified by ICEBERG_EXPLORER_CONFIG env var
2. Environment variable overrides with ICEBERG_EXPLORER_ prefix
   - Nested keys use double underscore: ICEBERG_EXPLORER_QUERY__MAX_ROWS
"""

from __future__ import annotations

import json
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class CatalogType(str, Enum):
    """Supported catalog types."""

    REST = "rest"
    LOCAL = "local"


class CatalogConfig(BaseSettings):
    """Configuration for Iceberg catalog connection."""

    model_config = SettingsConfigDict(
        env_prefix="ICEBERG_EXPLORER_CATALOG__",
        env_nested_delimiter="__",
    )

    type: CatalogType = CatalogType.REST
    uri: str = Field(default="http://localhost:8181", description="Catalog URI (for REST catalog)")
    warehouse: str = Field(default="", description="Warehouse path (for local catalog)")
    name: str = Field(default="default", description="Catalog name for DuckDB attachment")


class QueryConfig(BaseSettings):
    """Configuration for query execution."""

    model_config = SettingsConfigDict(
        env_prefix="ICEBERG_EXPLORER_QUERY__",
        env_nested_delimiter="__",
    )

    max_rows: int = Field(default=10000, ge=1, description="Maximum rows to return per query")
    default_timeout: int = Field(
        default=300, ge=10, le=3600, description="Default query timeout in seconds"
    )
    min_timeout: int = Field(default=10, ge=1, description="Minimum allowed query timeout")
    max_timeout: int = Field(default=3600, ge=1, description="Maximum allowed query timeout")


class DuckDBConfig(BaseSettings):
    """Configuration for DuckDB engine."""

    model_config = SettingsConfigDict(
        env_prefix="ICEBERG_EXPLORER_DUCKDB__",
        env_nested_delimiter="__",
    )

    memory_limit: str = Field(
        default="4GB", description="DuckDB memory limit (e.g., '4GB', '512MB')"
    )
    threads: int = Field(default=4, ge=1, description="Number of DuckDB threads")


class ServerConfig(BaseSettings):
    """Configuration for the HTTP server."""

    model_config = SettingsConfigDict(
        env_prefix="ICEBERG_EXPLORER_SERVER__",
        env_nested_delimiter="__",
    )

    host: str = Field(default="0.0.0.0", description="Server host")
    port: int = Field(default=8080, ge=1, le=65535, description="Server port")


class OTelConfig(BaseSettings):
    """Configuration for OpenTelemetry."""

    model_config = SettingsConfigDict(
        env_prefix="ICEBERG_EXPLORER_OTEL__",
        env_nested_delimiter="__",
    )

    enabled: bool = Field(default=False, description="Enable OpenTelemetry instrumentation")
    endpoint: str = Field(
        default="http://localhost:4317", description="OTLP exporter endpoint"
    )
    service_name: str = Field(default="iceberg-explorer", description="Service name for traces")


class Settings(BaseSettings):
    """Root configuration for Iceberg Explorer."""

    model_config = SettingsConfigDict(
        env_prefix="ICEBERG_EXPLORER_",
        env_nested_delimiter="__",
        env_file=None,
        extra="ignore",
    )

    catalog: CatalogConfig = Field(default_factory=CatalogConfig)
    query: QueryConfig = Field(default_factory=QueryConfig)
    duckdb: DuckDBConfig = Field(default_factory=DuckDBConfig)
    server: ServerConfig = Field(default_factory=ServerConfig)
    otel: OTelConfig = Field(default_factory=OTelConfig)

    @model_validator(mode="before")
    @classmethod
    def load_from_json_file(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Load configuration from JSON file if ICEBERG_EXPLORER_CONFIG is set."""
        import os

        config_path = os.environ.get("ICEBERG_EXPLORER_CONFIG")
        if config_path:
            path = Path(config_path)
            if not path.exists():
                raise ValueError(f"Config file not found: {config_path}")
            with path.open() as f:
                file_config = json.load(f)
            for key, value in file_config.items():
                if key not in data:
                    data[key] = value
                elif isinstance(value, dict) and isinstance(data.get(key), dict):
                    data[key] = {**value, **data[key]}
        return data


_settings: Settings | None = None


def get_settings() -> Settings:
    """Get the application settings (cached)."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def load_settings(config_path: str | Path | None = None) -> Settings:
    """Load settings, optionally from a specific config file.

    Args:
        config_path: Path to JSON config file. If None, uses ICEBERG_EXPLORER_CONFIG env var.

    Returns:
        Loaded Settings instance.
    """
    import os

    if config_path is not None:
        os.environ["ICEBERG_EXPLORER_CONFIG"] = str(config_path)

    global _settings
    _settings = Settings()
    return _settings


def reset_settings() -> None:
    """Reset cached settings (useful for testing)."""
    global _settings
    _settings = None

"""Bootstrap Lakekeeper catalog for devcontainer usage."""

from __future__ import annotations

import json
import time
from pathlib import Path
from urllib import error, request

LAKEKEEPER_BASE_URL = "http://lakekeeper:8181"
PROJECT_NAME = "default"
WAREHOUSE_NAME = "demo"


class LakekeeperError(RuntimeError):
    """Raised when Lakekeeper bootstrap fails."""


def _as_list(value: object | None) -> list[object]:
    if isinstance(value, list):
        return value
    return []


def _request(
    method: str,
    path: str,
    payload: dict[str, object] | None = None,
    expected_status: int | set[int] | None = None,
) -> dict[str, object] | None:
    url = f"{LAKEKEEPER_BASE_URL}{path}"
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"Content-Type": "application/json"} if payload is not None else {}
    req = request.Request(url, data=data, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=10) as response:
            status = response.status
            body = response.read().decode("utf-8")
    except error.HTTPError as exc:
        status = exc.code
        body = exc.read().decode("utf-8") if exc.fp else ""
    except error.URLError as exc:
        raise LakekeeperError(f"Failed to reach Lakekeeper at {url}: {exc}") from exc

    if expected_status is not None:
        expected = {expected_status} if isinstance(expected_status, int) else set(expected_status)
        if status not in expected:
            raise LakekeeperError(
                f"Unexpected status {status} for {method} {path}: {body or 'empty response'}"
            )

    if not body:
        return None
    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise LakekeeperError(
            f"Failed to parse JSON response from {method} {path}: {body}"
        ) from exc


def _wait_for_health() -> None:
    for _ in range(30):
        try:
            response = _request("GET", "/health", expected_status=200)
            if response is None:
                return
        except LakekeeperError:
            time.sleep(1)
        else:
            return
    raise LakekeeperError("Lakekeeper did not become healthy in time")


def _bootstrap_catalog() -> None:
    try:
        _request(
            "POST",
            "/management/v1/bootstrap",
            {
                "accept-terms-of-use": True,
                "user-email": "dev@example.com",
                "user-name": "Dev User",
                "is-operator": True,
            },
            expected_status={200, 204},
        )
    except LakekeeperError as exc:
        if "CatalogAlreadyBootstrapped" not in str(exc):
            raise


def _ensure_project() -> str:
    response = _request("GET", "/management/v1/project", expected_status=200) or {}
    if isinstance(response, dict):
        project_id = response.get("project-id")
        project_name = response.get("project-name")
        if project_id and project_name:
            return str(project_id)
        for project in _as_list(response.get("projects")):
            if isinstance(project, dict) and project.get("name") == PROJECT_NAME:
                return str(project["project-id"])

    response = _request(
        "POST",
        "/management/v1/project",
        {"project-name": PROJECT_NAME},
        expected_status={200, 201},
    )
    if not response or "project-id" not in response:
        raise LakekeeperError("Project creation did not return a project-id")
    return str(response["project-id"])


def _ensure_warehouse(project_id: str) -> str:
    response = _request("GET", "/management/v1/warehouse", expected_status=200) or {}
    if isinstance(response, dict):
        for warehouse in _as_list(response.get("warehouses")):
            if isinstance(warehouse, dict) and warehouse.get("name") == WAREHOUSE_NAME:
                return str(warehouse["warehouse-id"])

    response = _request(
        "POST",
        "/management/v1/warehouse",
        {
            "project-id": project_id,
            "warehouse-name": WAREHOUSE_NAME,
            "storage-profile": {
                "type": "s3",
                "bucket": "iceberg-warehouse",
                "region": "us-east-1",
                "endpoint": "http://minio:9000",
                "path-style-access": True,
                "sts-enabled": False,
            },
            "storage-credential": {
                "type": "s3",
                "credential-type": "access-key",
                "aws-access-key-id": "minioadmin",
                "aws-secret-access-key": "minioadmin",
            },
        },
        expected_status={200, 201},
    )
    if not response or "warehouse-id" not in response:
        raise LakekeeperError("Warehouse creation did not return a warehouse-id")
    return str(response["warehouse-id"])


def _write_catalog_config() -> None:
    config_path = Path.home() / ".config" / "iceberg-explorer" / "lakekeeper-config.json"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        json.dumps(
            {
                "catalog": {
                    "type": "rest",
                    "uri": f"{LAKEKEEPER_BASE_URL}/catalog",
                    "warehouse": WAREHOUSE_NAME,
                }
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def main() -> None:
    _wait_for_health()
    _bootstrap_catalog()
    project_id = _ensure_project()
    warehouse_id = _ensure_warehouse(project_id)
    _write_catalog_config()
    print(f"Lakekeeper configured with warehouse {warehouse_id}")


if __name__ == "__main__":
    main()

"""Tests for web UI routes."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from iceberg_explorer.main import app


@pytest.fixture
def client() -> TestClient:
    """Create test client."""
    return TestClient(app)


class TestIndexPage:
    """Tests for the index page."""

    def test_index_returns_html(self, client: TestClient) -> None:
        """Index page returns HTML content."""
        response = client.get("/")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    def test_index_contains_base_elements(self, client: TestClient) -> None:
        """Index page contains expected base layout elements."""
        response = client.get("/")
        content = response.text

        assert "Iceberg Explorer" in content
        assert "<!DOCTYPE html>" in content
        assert "htmx.org" in content
        assert "alpinejs" in content
        assert "tailwindcss" in content.lower() or "tailwind" in content.lower()

    def test_index_does_not_show_top_nav_tabs(self, client: TestClient) -> None:
        """Index page does not show legacy Catalog/Query top tabs."""
        response = client.get("/")
        content = response.text

        assert 'href="/query"' not in content

    def test_index_has_sidebar(self, client: TestClient) -> None:
        """Index page has sidebar for namespaces."""
        response = client.get("/")
        content = response.text

        assert "Namespaces" in content
        assert "sidebar" in content.lower()


class TestQueryRoute:
    """Tests for removed query route."""

    def test_query_route_is_not_available(self, client: TestClient) -> None:
        """Legacy query page route returns not found."""
        response = client.get("/query")
        assert response.status_code == 404


class TestNamespaceTreePartial:
    """Tests for the namespace tree partial."""

    def test_namespace_tree_returns_html(self, client: TestClient) -> None:
        """Namespace tree partial returns HTML content."""
        response = client.get("/ui/partials/namespace-tree")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    def test_namespace_tree_empty_state(self, client: TestClient) -> None:
        """Namespace tree shows empty state when no namespaces."""
        response = client.get("/ui/partials/namespace-tree")
        content = response.text

        assert "No namespaces found" in content or "namespace-item" in content


class TestNamespaceChildrenPartial:
    """Tests for the namespace children partial."""

    def test_namespace_children_returns_html(self, client: TestClient) -> None:
        """Namespace children partial returns HTML content."""
        response = client.get("/ui/partials/namespace-children?parent=test")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    def test_namespace_children_handles_empty_namespace(self, client: TestClient) -> None:
        """Namespace children shows empty state for non-existent namespace."""
        response = client.get("/ui/partials/namespace-children?parent=nonexistent")
        assert response.status_code == 200
        content = response.text
        assert "Empty namespace" in content


class TestTableDetailsPartial:
    """Tests for the table details partial."""

    def test_table_details_returns_html(self, client: TestClient) -> None:
        """Table details partial returns HTML content."""
        response = client.get("/ui/partials/table-details?table_path=ns.table")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    def test_table_details_invalid_path(self, client: TestClient) -> None:
        """Table details shows error for invalid path format."""
        response = client.get("/ui/partials/table-details?table_path=invalid")
        assert response.status_code == 200
        content = response.text
        assert "Invalid table path" in content or "Error" in content

    def test_table_details_shows_error_for_missing_table(self, client: TestClient) -> None:
        """Table details shows error when table doesn't exist."""
        response = client.get("/ui/partials/table-details?table_path=fake.missing_table")
        content = response.text
        # Should either show an error message or return a non-200 status
        assert ("Error" in content or "error" in content) or response.status_code != 200

    def test_table_details_includes_query_tab_and_prefill(self, client: TestClient) -> None:
        """Table details include query UI with default SQL for the selected table."""
        mock_catalog_service = MagicMock()
        mock_catalog_service.get_table_details.return_value = {
            "location": "s3://warehouse/sales/orders",
            "partition_spec": {"fields": []},
            "snapshots": [],
            "snapshot_id": None,
        }
        mock_catalog_service.get_table_schema.return_value = {
            "fields": [
                {
                    "name": "order_id",
                    "type": "bigint",
                    "nullable": False,
                    "field_id": 1,
                }
            ]
        }

        with patch(
            "iceberg_explorer.api.routes.ui.get_catalog_service",
            return_value=mock_catalog_service,
        ):
            response = client.get("/ui/partials/table-details?table_path=sales.orders")

        content = response.text
        assert response.status_code == 200
        assert "SQL Query Editor" in content
        assert "Run Query" in content
        assert "SELECT * FROM sales.orders LIMIT 100" in content


class TestResponsiveDesign:
    """Tests for responsive design elements."""

    def test_has_viewport_meta(self, client: TestClient) -> None:
        """Pages have viewport meta tag for responsive design."""
        response = client.get("/")
        content = response.text

        assert 'name="viewport"' in content
        assert "width=device-width" in content


class TestNamespaceTreeIntegration:
    """Integration tests for namespace tree functionality."""

    def test_index_page_loads_tree_via_htmx(self, client: TestClient) -> None:
        """Index page has HTMX trigger to load namespace tree."""
        response = client.get("/")
        content = response.text

        assert 'hx-get="/ui/partials/namespace-tree"' in content
        assert 'hx-trigger="load"' in content

    def test_namespace_tree_has_expand_collapse_structure(self, client: TestClient) -> None:
        """Namespace tree template has expand/collapse UI elements."""
        response = client.get("/ui/partials/namespace-tree")
        content = response.text

        assert "No namespaces found" in content or "namespace-item" in content

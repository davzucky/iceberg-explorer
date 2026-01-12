"""Tests for web UI routes."""

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

    def test_index_has_navigation(self, client: TestClient) -> None:
        """Index page has navigation links."""
        response = client.get("/")
        content = response.text

        assert 'href="/"' in content
        assert 'href="/query"' in content
        assert "Catalog" in content
        assert "Query" in content

    def test_index_has_sidebar(self, client: TestClient) -> None:
        """Index page has sidebar for namespaces."""
        response = client.get("/")
        content = response.text

        assert "Namespaces" in content
        assert "sidebar" in content.lower()


class TestQueryPage:
    """Tests for the query page."""

    def test_query_page_returns_html(self, client: TestClient) -> None:
        """Query page returns HTML content."""
        response = client.get("/query")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    def test_query_page_contains_base_elements(self, client: TestClient) -> None:
        """Query page contains expected base layout elements."""
        response = client.get("/query")
        content = response.text

        assert "Iceberg Explorer" in content
        assert "<!DOCTYPE html>" in content

    def test_query_page_has_editor_placeholder(self, client: TestClient) -> None:
        """Query page has SQL editor placeholder."""
        response = client.get("/query")
        content = response.text

        assert "SQL Query Editor" in content

    def test_query_page_has_results_placeholder(self, client: TestClient) -> None:
        """Query page has results placeholder."""
        response = client.get("/query")
        content = response.text

        assert "Results" in content

    def test_query_page_has_monaco_editor(self, client: TestClient) -> None:
        """Query page loads Monaco editor."""
        response = client.get("/query")
        content = response.text

        assert "monaco-editor" in content
        assert 'id="editor-container"' in content

    def test_query_page_has_run_button(self, client: TestClient) -> None:
        """Query page has Run Query button."""
        response = client.get("/query")
        content = response.text

        assert "Run Query" in content
        assert "executeQuery()" in content

    def test_query_page_has_cancel_button(self, client: TestClient) -> None:
        """Query page has Cancel button for stopping queries."""
        response = client.get("/query")
        content = response.text

        assert "Cancel" in content
        assert "cancelQuery()" in content

    def test_query_page_has_timeout_selector(self, client: TestClient) -> None:
        """Query page has timeout dropdown selector."""
        response = client.get("/query")
        content = response.text

        assert "Timeout:" in content
        assert "<select" in content
        assert 'value="30"' in content  # 30s option
        assert 'value="60"' in content  # 60s option
        assert 'value="300"' in content  # 5m option
        assert 'value="900"' in content  # 15m option
        assert 'value="3600"' in content  # 1h option

    def test_query_page_has_error_display_area(self, client: TestClient) -> None:
        """Query page has area for displaying errors."""
        response = client.get("/query")
        content = response.text

        assert "Query Error" in content
        assert "error" in content.lower()

    def test_query_page_has_alpine_js_component(self, client: TestClient) -> None:
        """Query page has Alpine.js queryEditor component."""
        response = client.get("/query")
        content = response.text

        assert "queryEditor()" in content
        assert "x-data" in content
        assert "x-init" in content

    def test_query_page_has_keyboard_shortcut(self, client: TestClient) -> None:
        """Query page supports Ctrl+Enter keyboard shortcut."""
        response = client.get("/query")
        content = response.text

        assert "KeyMod.CtrlCmd" in content or "CtrlCmd" in content
        assert "KeyCode.Enter" in content or "Enter" in content


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

        assert "No namespaces found" in content


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
        content = response.text
        assert "Invalid table path" in content or "Error" in content

    def test_table_details_shows_error_for_missing_table(self, client: TestClient) -> None:
        """Table details shows error when table doesn't exist."""
        response = client.get("/ui/partials/table-details?table_path=fake.missing_table")
        content = response.text
        # Should either show an error message or return a non-200 status
        assert ("Error" in content or "error" in content) or response.status_code != 200


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

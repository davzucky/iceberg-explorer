"""Web UI routes for Iceberg Explorer."""

from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

TEMPLATES_DIR = Path(__file__).parent.parent.parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

router = APIRouter(tags=["ui"])


@router.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    """Render the main catalog browser page."""
    return templates.TemplateResponse(request, "index.html")


@router.get("/query", response_class=HTMLResponse)
async def query_page(request: Request) -> HTMLResponse:
    """Render the query editor page."""
    return templates.TemplateResponse(request, "query.html")


@router.get("/ui/partials/namespace-tree", response_class=HTMLResponse)
async def namespace_tree_partial(request: Request) -> HTMLResponse:
    """Render the namespace tree partial (placeholder for US-015)."""
    return templates.TemplateResponse(
        request,
        "partials/namespace_tree.html",
        {"namespaces": []},
    )

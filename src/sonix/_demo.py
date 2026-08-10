"""The app `uv run sonix` serves when given no target.

Kept inside the package (rather than under examples/) so that a bare
``sonix`` works from an installed wheel, with no repository checkout and
nothing on sys.path. Real examples live in examples/.
"""

from __future__ import annotations

from sonix.app.applications import Sonix
from sonix.app.requests import Request

app = Sonix()


@app.get("/")
async def hello_world(request: Request) -> dict:
    return {"message": "Hello, world!"}


@app.get("/items/{item_id:int}")
async def get_item(request: Request) -> dict:
    return {"item_id": request.path_params["item_id"]}

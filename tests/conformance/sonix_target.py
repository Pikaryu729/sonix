"""A Sonix application for another ASGI server to run.

Kept as an importable module rather than built inline, because
test_sonix_under_uvicorn.py launches uvicorn as a subprocess and hands it an
import string -- exactly the way a real deployment would.
"""

from __future__ import annotations

import contextlib

from sonix import JSONResponse, Request, Sonix
from sonix.app.exceptions import HTTPException


@contextlib.asynccontextmanager
async def lifespan(app):
    yield {"greeting": "from lifespan"}


app = Sonix(lifespan=lifespan)


@app.get("/ping")
async def ping(request: Request) -> str:
    return "pong"


@app.get("/items/{item_id:int}")
async def read_item(request: Request) -> dict:
    return {"item_id": request.path_params["item_id"]}


@app.post("/echo")
async def echo(request: Request) -> dict:
    return {"echoed": await request.json()}


@app.get("/state")
async def state(request: Request) -> dict:
    # Proves the foreign server drove Sonix's lifespan and that the state it
    # yielded reached a request scope.
    return {"greeting": request.scope["state"]["greeting"]}


@app.get("/query")
async def query(request: Request) -> dict:
    return {"q": request.query_params.get("q")}


@app.get("/teapot")
async def teapot(request: Request) -> JSONResponse:
    raise HTTPException(418, "I'm a teapot")


@app.get("/boom")
async def boom(request: Request) -> str:
    raise RuntimeError("deliberate failure")

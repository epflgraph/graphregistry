# graphregistry/entrypoints/api/main.py
from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, RedirectResponse

from graphregistry.entrypoints.api.router import router


logger = logging.getLogger("uvicorn.error")


def create_app() -> FastAPI:
    app = FastAPI(
        title="GraphRegistry API",
        summary="HTTP API for graph registry node, edge, and subgraph operations.",
        version="0.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    app.include_router(router)

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request,
        exc: RequestValidationError,
    ) -> JSONResponse:
        body = await request.body()

        logger.error("Validation error on %s %s", request.method, request.url.path)
        logger.error("Errors: %s", exc.errors())
        logger.error("Body: %s", body.decode("utf-8", errors="replace"))

        return JSONResponse(
            status_code=422,
            content={"detail": exc.errors()},
        )

    @app.get("/", include_in_schema=False)
    def root() -> RedirectResponse:
        return RedirectResponse(url="/docs")

    @app.get("/health", tags=["system"])
    def healthcheck() -> dict[str, str]:
        return {"status": "ok"}

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "graphregistry.entrypoints.api.main:app",
        host="0.0.0.0",
        port=8000,
    )

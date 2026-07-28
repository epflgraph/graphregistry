# graphregistry/entrypoints/api/main.py
from __future__ import annotations
import json
import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import ValidationError
from graphregistry.entrypoints.api.router import router

# import global config
from graphregistry.common.config import GlobalConfig
from graphregistry.common.version import REGISTRY_API_VERSION
from graphregistry.domain.exceptions import DisallowedTypeError

# Set up logging
logger = logging.getLogger("uvicorn.error")


# Fields that carry node/edge object types in the request schemas.
_OBJECT_TYPE_FIELDS = {"type", "from_type", "to_type"}


def _is_object_type_literal_error(error: dict[str, Any]) -> bool:
    """Detect Pydantic literal errors on object-type fields."""
    if error.get("type") != "literal_error":
        return False
    loc = error.get("loc")
    return bool(loc) and loc[-1] in _OBJECT_TYPE_FIELDS


def _build_type_error_detail(body: bytes, errors: list[dict[str, Any]]) -> str | list[str] | None:
    """Convert object-type literal errors into unified 'not an allowed type' messages.

    Returns a single message when there is one invalid type, a list otherwise.
    Returns ``None`` when the errors are not purely object-type literal errors.
    """
    if not errors or not all(_is_object_type_literal_error(e) for e in errors):
        return None

    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        return None

    messages: list[str] = []
    for error in errors:
        loc = error.get("loc", ())
        value = error.get("input")
        field = loc[-1] if loc else None

        if field == "type":
            messages.append(f"Node type '{value}' is not an allowed type.")
        elif field in ("from_type", "to_type"):
            try:
                if "edge_list" in loc:
                    idx = loc[loc.index("edge_list") + 1]
                    edge = payload["edge_list"][idx]
                else:
                    edge = payload["edge"]
                edge_tuple = (
                    edge.get("from_type"),
                    edge.get("to_type"),
                    edge.get("context", "part of"),
                )
                msg = f"Edge type {edge_tuple} is not an allowed type."
            except Exception:
                msg = f"Edge type '{value}' is not an allowed type."
            if msg not in messages:
                messages.append(msg)
        else:
            messages.append(f"Type '{value}' is not an allowed type.")

    if not messages:
        return None
    return messages[0] if len(messages) == 1 else messages


# Create the FastAPI application
def create_app() -> FastAPI:

    # Load configuration when the app is created, not at import time.
    glbcfg = GlobalConfig()

    # Initialize the FastAPI app with metadata and documentation settings
    app = FastAPI(
        title       = glbcfg.api_title,
        summary     = glbcfg.api_summary,
        version     = REGISTRY_API_VERSION,
        docs_url    = "/docs",
        redoc_url   = "/redoc",
        openapi_url = "/openapi.json",
    )

    # Include the API router which contains all the endpoint definitions
    app.include_router(router)

    #====================#
    # Exception handlers #
    #====================#

    @app.exception_handler(RequestValidationError)
    async def request_validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        """
        Handle FastAPI request validation errors.

        These happen before the endpoint function is called, usually because
        the incoming JSON body, query params, or path params do not match the
        declared request schema.
        """

        body = await request.body()

        # If the only validation failures are invalid object types, return the
        # same unified "not an allowed type" message that the allowed-types
        # validator uses, instead of a Pydantic literal_error payload.
        type_detail = _build_type_error_detail(body, exc.errors())
        if type_detail is not None:
            logger.warning(
                "Invalid object type in API request: method=%s path=%s detail=%s",
                request.method,
                request.url.path,
                type_detail,
            )
            return JSONResponse(
                status_code=400,
                content={"detail": type_detail},
            )

        logger.error(
            "Request validation error: method=%s path=%s",
            request.method,
            request.url.path,
        )
        logger.error("Errors: %s", exc.errors())
        logger.error("Body: %s", body.decode("utf-8", errors="replace"))

        return JSONResponse(
            status_code=422,
            content={
                "detail": exc.errors(),
            },
        )

    @app.exception_handler(ValidationError)
    async def pydantic_validation_exception_handler(request: Request, exc: ValidationError) -> JSONResponse:
        """
        Handle Pydantic validation errors raised inside endpoint code.

        These are different from RequestValidationError. They usually mean
        that the API received a valid request, but our own conversion logic
        failed while building a domain model or response object.
        """

        logger.exception(
            "Internal Pydantic validation error: method=%s path=%s",
            request.method,
            request.url.path,
        )

        return JSONResponse(
            status_code=500,
            content={
                "detail": f"Internal validation error: {type(exc).__name__}: {exc}",
            },
        )

    @app.exception_handler(ValueError)
    async def value_error_exception_handler(request: Request, exc: ValueError) -> JSONResponse:
        """
        Handle expected ValueError exceptions as bad API requests.

        This is useful for errors such as unknown configured environments,
        unsupported object types, malformed keys, etc.

        Later, this can be tightened by replacing broad ValueError handling
        with a custom BadAPIRequestError.
        """

        logger.warning(
            "Invalid API request: method=%s path=%s error=%s",
            request.method,
            request.url.path,
            exc,
            exc_info=True,
        )

        return JSONResponse(
            status_code=400,
            content={
                "detail": f"Invalid request: {type(exc).__name__}: {exc}",
            },
        )

    @app.exception_handler(DisallowedTypeError)
    async def disallowed_type_exception_handler(request: Request, exc: DisallowedTypeError) -> JSONResponse:
        """
        Handle attempts to save nodes or edges that are not in the configured
        allow-list.

        Returns a clear 400 Bad Request message so callers know the type is
        disallowed.
        """

        logger.warning(
            "Disallowed type in API request: method=%s path=%s error=%s",
            request.method,
            request.url.path,
            exc,
        )

        return JSONResponse(
            status_code=400,
            content={
                "detail": str(exc),
            },
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        """
        Handle unexpected errors.

        This keeps endpoint code clean while ensuring unexpected exceptions
        are logged with tracebacks and returned as structured 500 responses.
        """

        logger.exception(
            "Unhandled API error: method=%s path=%s",
            request.method,
            request.url.path,
        )

        return JSONResponse(
            status_code=500,
            content={
                "detail": f"Internal server error: {type(exc).__name__}: {exc}",
            },
        )

    #==================#
    # Utility routes   #
    #==================#

    # Define a root endpoint that redirects to the API documentation for easy access
    @app.get("/", include_in_schema=False)
    def root() -> RedirectResponse:
        return RedirectResponse(url="/docs")

    # Define a health check endpoint that returns a simple status message
    # to indicate the service is running
    @app.get("/health", tags=["system"])
    def healthcheck() -> dict[str, str]:
        return {"status": "ok"}

    # Return the configured FastAPI application instance
    return app

# If this script is run directly (e.g., via `python main.py`),
# start the Uvicorn server to serve the FastAPI application.
# The app is created lazily via the factory so importing this module does not
# trigger configuration loading or database connections.
if __name__ == "__main__":

    # Import Uvicorn, the ASGI server used to run FastAPI applications
    import uvicorn

    # Run the Uvicorn server using the application factory.
    uvicorn.run(
        "graphregistry.entrypoints.api.main:create_app",
        host="0.0.0.0",
        port=8000,
        factory=True,
    )

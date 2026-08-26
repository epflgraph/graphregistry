# graphregistry/entrypoints/api/main.py
from __future__ import annotations
import json
import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import ValidationError

# import global config
from graphregistry.common.config import APIConfig, GlobalConfig
from graphregistry.common.version import REGISTRY_API_VERSION
from graphregistry.domain.exceptions import (
    ConnectionExhaustedError,
    DisallowedTypeError,
    DuplicateKeyError,
    LockWaitTimeoutError,
    PersistenceError,
)
from graphregistry.entrypoints.api.router import router
from graphregistry.entrypoints.dependencies import build_db

# Set up logging
logger = logging.getLogger("uvicorn.error")


# Fields that carry node/edge object types in the request schemas.
_OBJECT_TYPE_FIELDS = {"type", "from_type", "to_type"}


#================================================================#
# Function Group: Validation error helpers                       #
#================================================================#

# Function: Detect Pydantic literal errors on object-type fields.
def _is_object_type_literal_error(error: dict[str, Any]) -> bool:
    """Detect Pydantic literal errors on object-type fields."""
    if error.get("type") != "literal_error":
        return False
    loc = error.get("loc")
    return bool(loc) and loc[-1] in _OBJECT_TYPE_FIELDS


# Function: Convert object-type literal errors into unified 'not an allowed type' messages.
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


# Function: Initialize process-scoped resources and clean them up on shutdown.
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize process-scoped resources and clean them up on shutdown."""
    app.state.db = build_db()
    yield
    # The GraphDB engine pool is cleaned up automatically when the process
    # exits. Explicit disposal can be added here later if needed.


#================================================================#
# Function Group: Application factory                            #
#================================================================#

# Function: Create and configure the FastAPI application.
def create_app() -> FastAPI:

    # Load configuration when the app is created, not at import time.
    glbcfg = GlobalConfig()
    api_cfg = APIConfig()

    # Initialize the FastAPI app with metadata and documentation settings
    app = FastAPI(
        title       = glbcfg.api_title,
        summary     = glbcfg.api_summary,
        version     = REGISTRY_API_VERSION,
        docs_url    = "/docs",
        redoc_url   = "/redoc",
        openapi_url = "/openapi.json",
        lifespan    = lifespan,
    )

    # Include the API router which contains all the endpoint definitions
    app.include_router(router)

    #================================================================#
    # Function Group: Exception handlers                             #
    #================================================================#

    # Function: Handle FastAPI request validation errors.
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

    # Function: Handle Pydantic validation errors raised inside endpoint code.
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

    # Function: Handle expected ValueError exceptions as bad API requests.
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

    # Function: Handle attempts to save disallowed node or edge types.
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

    # Function: Handle database connection exhaustion (MySQL 1040).
    @app.exception_handler(ConnectionExhaustedError)
    async def connection_exhausted_exception_handler(request: Request, exc: ConnectionExhaustedError) -> JSONResponse:
        """Handle database connection exhaustion (MySQL 1040)."""
        logger.warning(
            "Database connection exhausted: method=%s path=%s error=%s",
            request.method,
            request.url.path,
            exc,
        )
        return JSONResponse(
            status_code=503,
            headers={"Retry-After": "10"},
            content={
                "detail": "Database is temporarily overloaded. Please retry after a short delay.",
            },
        )

    # Function: Handle lock wait timeouts (MySQL 1205).
    @app.exception_handler(LockWaitTimeoutError)
    async def lock_wait_timeout_exception_handler(request: Request, exc: LockWaitTimeoutError) -> JSONResponse:
        """Handle lock wait timeouts (MySQL 1205)."""
        logger.warning(
            "Lock wait timeout in API request: method=%s path=%s error=%s",
            request.method,
            request.url.path,
            exc,
        )
        return JSONResponse(
            status_code=503,
            headers={"Retry-After": "5"},
            content={
                "detail": "Database lock wait timeout. Please retry after a short delay.",
            },
        )

    # Function: Handle duplicate key violations (MySQL 1062).
    @app.exception_handler(DuplicateKeyError)
    async def duplicate_key_exception_handler(request: Request, exc: DuplicateKeyError) -> JSONResponse:
        """Handle duplicate key violations (MySQL 1062)."""
        logger.warning(
            "Duplicate key in API request: method=%s path=%s error=%s",
            request.method,
            request.url.path,
            exc,
        )
        return JSONResponse(
            status_code=409,
            content={
                "detail": f"Duplicate key: {exc}",
            },
        )

    # Function: Handle generic persistence errors not matched above.
    @app.exception_handler(PersistenceError)
    async def persistence_error_exception_handler(request: Request, exc: PersistenceError) -> JSONResponse:
        """Handle generic persistence errors that were not matched above."""
        logger.error(
            "Persistence error in API request: method=%s path=%s error=%s",
            request.method,
            request.url.path,
            exc,
            exc_info=True,
        )
        return JSONResponse(
            status_code=500,
            content={
                "detail": f"Persistence error: {exc}",
            },
        )

    # Function: Handle unexpected errors as structured 500 responses.
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

    #================================================================#
    # Function Group: OpenAPI / Swagger customisation                #
    #================================================================#

    def custom_openapi() -> dict[str, Any]:
        """Generate the OpenAPI schema and override object-type examples.

        The default Pydantic examples come from the ObjectType literal, whose
        first value is "Category". We replace them with the first allowed type
        from ``config_api.json`` so Swagger shows realistic, permitted values.
        """
        if app.openapi_schema is not None:
            return app.openapi_schema

        openapi_schema = get_openapi(
            title=app.title,
            version=app.version,
            summary=app.summary,
            description=app.description,
            routes=app.routes,
        )

        schemas = openapi_schema.setdefault("components", {}).setdefault("schemas", {})

        # Node examples: first allowed node type.
        if api_cfg.allowed_node_types_list:
            example_node_type = api_cfg.allowed_node_types_list[0]
            for schema_name in ("NodeSpec", "NodeKeySpec"):
                schema = schemas.get(schema_name)
                if schema and "type" in schema.get("properties", {}):
                    schema["properties"]["type"]["example"] = example_node_type

        # Edge examples: first allowed edge tuple (from_type, to_type, context).
        if api_cfg.allowed_edge_tuples_list:
            example_edge_tuple = api_cfg.allowed_edge_tuples_list[0]
            for schema_name in ("EdgeSpec", "EdgeKeySpec"):
                schema = schemas.get(schema_name)
                if schema is None:
                    continue
                properties = schema.get("properties", {})
                for field_name, field_value in zip(
                    ("from_type", "to_type", "context"), example_edge_tuple
                ):
                    if field_name in properties:
                        properties[field_name]["example"] = field_value

        # Swagger UI sometimes prefers a default value over a property-level
        # example, so also set full request-level examples for the save endpoints.
        if api_cfg.allowed_node_types_list:
            example_node = {
                "type": example_node_type,
                "subtype": "string",
                "id": "string",
                "short_code": "string",
                "title": "string",
                "description": "string",
                "url": "string",
                "custom_fields": [
                    {
                        "field_language": "n/a",
                        "field_name": "string",
                        "field_value": "string",
                    }
                ],
            }
            node_save_request = schemas.get("APINodesSaveRequest")
            if node_save_request is not None:
                node_save_request["example"] = {"node": example_node}
            node_save_many_request = schemas.get("APINodesSaveManyRequest")
            if node_save_many_request is not None:
                node_save_many_request["example"] = {"node_list": [example_node]}

        if api_cfg.allowed_edge_tuples_list:
            from_type, to_type, context = example_edge_tuple
            example_edge = {
                "from_type": from_type,
                "from_id": "string",
                "to_type": to_type,
                "to_id": "string",
                "context": context,
                "custom_fields": [
                    {
                        "field_language": "n/a",
                        "field_name": "string",
                        "field_value": "string",
                    }
                ],
            }
            edge_save_request = schemas.get("APIEdgesSaveRequest")
            if edge_save_request is not None:
                edge_save_request["example"] = {"edge": example_edge}
            edge_save_many_request = schemas.get("APIEdgesSaveManyRequest")
            if edge_save_many_request is not None:
                edge_save_many_request["example"] = {"edge_list": [example_edge]}

        app.openapi_schema = openapi_schema
        return app.openapi_schema

    app.openapi = custom_openapi

    #================================================================#
    # Function Group: Utility routes                                 #
    #================================================================#

    # Function: Redirect the root path to the API documentation.
    @app.get("/", include_in_schema=False)
    def root() -> RedirectResponse:
        return RedirectResponse(url="/docs")

    # Function: Return a simple health-check response.
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

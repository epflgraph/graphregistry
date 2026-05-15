# graphregistry/entrypoints/api/main.py
from __future__ import annotations
import logging
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import ValidationError
from graphregistry.entrypoints.api.router import router

# import global config
from graphregistry.common.config import GlobalConfig
glbcfg = GlobalConfig()

# Set up logging
logger = logging.getLogger("uvicorn.error")

# Create the FastAPI application
def create_app() -> FastAPI:

    # Initialize the FastAPI app with metadata and documentation settings
    app = FastAPI(
        title       = glbcfg.api_title,
        summary     = glbcfg.api_summary,
        version     = "0.1.0",
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

# Create the FastAPI application instance by calling the create_app function
app = create_app()

# If this script is run directly (e.g., via `python main.py`),
# start the Uvicorn server to serve the FastAPI application
if __name__ == "__main__":

    # Import Uvicorn, the ASGI server used to run FastAPI applications
    import uvicorn

    # Run the Uvicorn server, specifying the application to run, host, and port
    uvicorn.run(
        "graphregistry.entrypoints.api.main:app",
        host="0.0.0.0",
        port=8000,
    )

# graphregistry/entrypoints/api/main.py
from __future__ import annotations
import logging
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, RedirectResponse
from graphregistry.entrypoints.api.router import router

# Set up logging
logger = logging.getLogger("uvicorn.error")

# Create the FastAPI application
def create_app() -> FastAPI:

    # Initialize the FastAPI app with metadata and documentation settings
    app = FastAPI(
        title       = "GraphRegistry API",
        summary     = "HTTP API for graph registry node, edge, and subgraph operations.",
        version     = "0.1.0",
        docs_url    = "/docs",
        redoc_url   = "/redoc",
        openapi_url = "/openapi.json",
    )

    # Include the API router which contains all the endpoint definitions
    app.include_router(router)

    # Define a custom exception handler for request validation errors to
    # log details and return a structured response
    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:

        # Read the request body for logging purposes (note: this may consume the body stream)
        body = await request.body()

        # Log the validation error details, including the request method, URL, errors, and body content
        logger.error("Validation error on %s %s", request.method, request.url.path)
        logger.error("Errors: %s", exc.errors())
        logger.error("Body: %s", body.decode("utf-8", errors="replace"))

        # Return a JSON response with the validation errors and a 422 status code
        return JSONResponse(status_code=422, content={"detail": exc.errors()})

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
    uvicorn.run("graphregistry.entrypoints.api.main:app", host="0.0.0.0", port=8000)

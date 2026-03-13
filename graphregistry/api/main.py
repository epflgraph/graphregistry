#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from fastapi import FastAPI
from graphregistry.api.router import router  # Corrected import path

from fastapi.exceptions import RequestValidationError
from fastapi import Request
from fastapi.responses import JSONResponse
import logging

logger = logging.getLogger("uvicorn.error")

app = FastAPI()

# Include the router
app.include_router(router)

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    body = await request.body()
    logger.error("422 on %s %s", request.method, request.url.path)
    logger.error("Validation errors: %s", exc.errors())
    logger.error("Request body: %s", body.decode("utf-8", errors="replace"))
    return JSONResponse(status_code=422, content={"detail": exc.errors()})

@app.get("/")
def read_root():
    return {"message": "FastAPI is running!"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

from fastapi import FastAPI
from api.registry.router import router  # Corrected import path

app = FastAPI()

# Include the router
app.include_router(router)

@app.get("/")
def read_root():
    return {"message": "FastAPI is running!"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

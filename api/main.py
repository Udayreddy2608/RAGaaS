from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager
from api.core.db import database, create_schema_if_missing
from api.routers.upload import router

@asynccontextmanager
async def lifespan(app: FastAPI):
    await database.connect()     
    create_schema_if_missing() 
    yield
    await database.disconnect()


app = FastAPI(
    title="RAG Brain",
    description="Upload documents and query them using RAG.",
    version="0.1.0",
    lifespan= lifespan
)




@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <h1> RagBrain </h1>
    <p>Status: Running</p>
    <a href="/docs">Swagger Docs</a>
    """

@app.get("/health")
def health():
    return {"status": "ok"}

app.include_router(router)
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

app = FastAPI(
    title="RAG Brain",
    description="Upload documents and query them using RAG.",
    version="0.1.0",
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
"""FastAPI application entry point."""
from fastapi import FastAPI

app = FastAPI(
    title="Validacao-BackEnd",
    description="API para comparação de dados entre queries no Redshift",
    version="0.1.0",
)


@app.get("/")
async def root():
    """Root endpoint - API information."""
    return {
        "name": "Validacao-BackEnd",
        "version": "0.1.0",
        "status": "running",
    }

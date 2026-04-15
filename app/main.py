"""FastAPI application entry point."""
from fastapi import FastAPI

from app.database import test_connection

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


@app.get("/health")
async def health_check():
    """Health check endpoint - verifies API and database status.

    Returns:
        dict: Health status with keys:
            - status: "ok" if everything is healthy, "degraded" if db is down
            - api: Always "running"
            - database: Connection test result from test_connection()
    """
    db_status = test_connection()

    return {
        "status": "ok" if db_status["connected"] else "degraded",
        "api": "running",
        "database": db_status,
    }

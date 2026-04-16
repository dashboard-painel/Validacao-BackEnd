"""FastAPI application entry point."""
import logging
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.database import test_connection
from app.local_db import init_local_db
from app.routers import comparar

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gerencia o ciclo de vida da aplicação.

    Startup: Inicializa as tabelas do banco de dados local.
    Shutdown: (nenhuma ação necessária)
    """
    init_local_db()
    yield


app = FastAPI(
    title="Validacao-BackEnd",
    description="API para comparação de dados entre queries no Redshift",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS — permite que o frontend existente consuma a API
# CORS_ORIGINS pode ser "*" ou lista separada por vírgulas
cors_origins_env = os.getenv("CORS_ORIGINS", "*")
allow_origins = ["*"] if cors_origins_env == "*" else [o.strip() for o in cors_origins_env.split(",")]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(comparar.router)


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

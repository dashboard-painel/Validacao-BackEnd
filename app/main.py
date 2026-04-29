import logging
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.database import test_connection
from app.local_db import init_local_db, close_pool
from app.routers import comparar

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gerencia o ciclo de vida da aplicação.

    Startup: Verifica PostgreSQL local e inicializa tabelas.
    Shutdown: Fecha o pool de conexões.
    """
    try:
        init_local_db()
        logger.info("✅ PostgreSQL local conectado e tabelas inicializadas")
    except Exception as e:
        logger.error(
            "❌ Falha ao conectar no PostgreSQL local: %s: %s. "
            "Verifique se o PostgreSQL está rodando e as variáveis LOCAL_DB_URL, LOCAL_DB_USER, LOCAL_DB_PASS estão corretas.",
            type(e).__name__, e,
        )
        raise SystemExit(1)
    yield
    close_pool()


app = FastAPI(
    title="Validacao-BackEnd",
    description="API para comparação de dados entre queries no Redshift",
    version="0.1.0",
    lifespan=lifespan,
)

cors_origins_env = os.getenv("CORS_ORIGINS", "*")
if cors_origins_env == "*":
    allow_origins = ["*"]
    allow_credentials = False
else:
    allow_origins = [o.strip() for o in cors_origins_env.split(",")]
    allow_credentials = True

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(comparar.router)


@app.get("/")
async def root():
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

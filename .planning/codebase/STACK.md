# Technology Stack

**Analysis Date:** 2025-01-31

## Languages

**Primary:**
- Python 3.12+ — All application code (confirmed by `README.md` requirement statement)

**No secondary languages detected** — pure Python backend.

## Runtime

**Environment:**
- Python 3.12+ (minimum required; confirmed in `README.md`)
- Virtual environment managed via `.venv/` (standard `venv`)

**Package Manager:**
- `pip` — dependencies declared in `requirements.txt`
- Lockfile: Not present (no `pip-tools`/`poetry.lock`/`uv.lock`)

## Frameworks

**Core:**
- `fastapi>=0.109.0` — HTTP API framework; application defined in `app/main.py` as `FastAPI(title="Validacao-BackEnd", version="0.1.0")`
- `uvicorn[standard]>=0.27.0` — ASGI server; run command: `uvicorn app.main:app --reload`
- `pydantic>=2.0.0` — Request/response validation; all API schemas in `app/schemas.py` extend `pydantic.BaseModel`

**Build/Dev:**
- No dedicated build tool — project is run directly with `uvicorn`
- No `Dockerfile` or `docker-compose.yml` detected

## Key Dependencies

**Critical:**
- `redshift-connector>=2.0.0` — Primary Redshift client; used in `app/database.py` via `redshift_connector.connect()`; default port `5439`
- `psycopg2-binary>=2.9.0` — PostgreSQL client for local persistence DB; used in `app/local_db.py` via `psycopg2.connect()` and `psycopg2.extras.RealDictCursor`
- `python-dotenv>=1.0.0` — Environment variable loading; `load_dotenv()` called in `app/main.py`, `app/database.py`, and `app/clients/coletor_bi.py`
- `requests>=2.31.0` — HTTP client for external API calls; used in `app/clients/business_connect.py` and `app/clients/coletor_bi.py`

**Infrastructure:**
- `fastapi.middleware.cors.CORSMiddleware` — CORS configured in `app/main.py`; origins driven by `CORS_ORIGINS` env var (default: `*`)
- `concurrent.futures.ThreadPoolExecutor` — Parallel HTTP calls to Business Connect and Coletor BI APIs (max 10 workers each); used in `app/clients/business_connect.py` and `app/clients/coletor_bi.py`

## Application Structure

**Entry Point:** `app/main.py`
- Creates `FastAPI` app instance
- Registers `CORSMiddleware`
- Mounts router: `app.routers.comparar`
- `lifespan` context manager calls `init_local_db()` on startup

**Layer Summary:**

| Layer | Path | Responsibility |
|-------|------|---------------|
| Router | `app/routers/comparar.py` | HTTP endpoints (GET/POST `/comparar`, `GET /historico`, etc.) |
| Service | `app/services/comparacao_service.py` | Orchestrates comparison, external API calls, persistence |
| Repository (Redshift) | `app/repositories/redshift_repository.py` | SQL query execution against Redshift |
| Repository (local) | `app/repositories/comparacao_repository.py` | Thin facade over `app/local_db.py` |
| Persistence | `app/local_db.py` | Raw psycopg2 SQL against local PostgreSQL |
| Clients | `app/clients/business_connect.py`, `app/clients/coletor_bi.py` | HTTP calls to external APIs |
| Models | `app/models/comparacao.py` | Internal domain dataclasses (`Divergencia`, `ResultadoComparacao`) |
| Schemas | `app/schemas.py` | Pydantic API request/response models |
| Mapper | `app/mappers/comparacao_mapper.py` | Converts domain models → Pydantic response schemas |
| Utilities | `app/utils.py` | `camadas_atrasadas()` — date delay/gap logic |
| DB connection | `app/database.py` | `get_connection()` context manager for Redshift |

## Configuration

**Environment:**
- Loaded via `python-dotenv` (`load_dotenv()`) from `.env` file at project root
- Template provided at `.env.example`
- No config class/schema — raw `os.getenv()` calls throughout modules

**Build:**
- No build configuration files (no `pyproject.toml`, `setup.py`, `Makefile`)
- No CI/CD pipeline detected (`.github/` contains GSD agent tooling only, no `workflows/`)

## Logging

- `logging.basicConfig` configured in `app/main.py`:
  - Level: `INFO`
  - Format: `%(asctime)s [%(levelname)s] %(name)s: %(message)s`
  - Date format: `%Y-%m-%d %H:%M:%S`
- Module-level loggers via `logging.getLogger(__name__)` in all layers
- Performance timing logged with emoji prefixes: `⏳ Aguardando...` / `✅ respondeu em Xs`

## Platform Requirements

**Development:**
- Python 3.12+
- Access to Amazon Redshift cluster
- Local PostgreSQL instance (default: `postgresql://root:root@localhost:5432/valida`)
- Network access to Business Connect API (`https://business-connect.triercloud.com.br/v1`)
- Network access to Coletor BI API (URL from `COLETOR_URL` env var)

**Production:**
- No deployment configuration detected
- No `Dockerfile`, no cloud deployment manifests
- API defaults to `http://localhost:8000` when run with `uvicorn app.main:app`
- Interactive docs available at `/docs` (Swagger UI) and `/redoc` (ReDoc)

---

*Stack analysis: 2025-01-31*

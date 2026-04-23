# Architecture

**Analysis Date:** 2025-01-31

## Pattern Overview

**Overall:** Layered Architecture (Clean / Onion-lite)

The application follows a strict top-down layered pattern with clear separation of concerns:

```
HTTP Request
    └── Router (app/routers/)
        └── Service (app/services/)
            ├── Repository/Redshift (app/repositories/redshift_repository.py)
            ├── Repository/Local (app/repositories/comparacao_repository.py)
            ├── Clients/External APIs (app/clients/)
            └── Mapper (app/mappers/)
                └── Schemas/Response (app/schemas.py)
```

**Key Characteristics:**
- Routers are thin — delegate immediately to services
- Services own all orchestration logic; they are the "use case" layer
- Repositories are the only layer that touches databases or external HTTP clients
- `comparacao_repository.py` is a pure facade/adapter over `local_db.py` and `clients/`
- Domain models (`app/models/`) are plain Python `@dataclass`s, separate from API schemas
- Mappers translate domain models → Pydantic API schemas (no business logic in mappers)

---

## Layers

### Router Layer
- **Purpose:** HTTP entry/exit point. Validate HTTP concerns, delegate, handle exceptions.
- **Location:** `app/routers/comparar.py`
- **Contains:** FastAPI `APIRouter`, endpoint functions, HTTP error handling (raises `HTTPException`)
- **Depends on:** `app/services/comparacao_service.py`, `app/repositories/comparacao_repository.py`, `app/mappers/comparacao_mapper.py`, `app/schemas.py`
- **Used by:** `app/main.py` via `app.include_router(comparar.router)`

### Service Layer
- **Purpose:** Orchestrates the full comparison workflow. The only layer that coordinates multiple repositories and clients in sequence.
- **Location:** `app/services/comparacao_service.py`
- **Contains:** `executar_comparacao()` (public entrypoint), `_comparar_resultados()` (private Redshift comparison)
- **Depends on:** `app/repositories/redshift_repository.py`, `app/repositories/comparacao_repository.py`, `app/clients/coletor_bi.py`, `app/mappers/comparacao_mapper.py`, `app/utils.py`
- **Used by:** `app/routers/comparar.py`

### Repository Layer — Redshift
- **Purpose:** Executes parameterized SQL against Amazon Redshift. Returns raw `list[dict]`.
- **Location:** `app/repositories/redshift_repository.py`
- **Contains:** `execute_gold_vendas()`, `execute_silver_stgn_dedup()`, `execute_cadfilia_por_codigos()`
- **Depends on:** `app/database.py` (connection context manager)
- **Used by:** `app/services/comparacao_service.py`

### Repository Layer — Local DB Facade
- **Purpose:** Facade/adapter that exposes a clean function API over `local_db.py` and `clients/business_connect.py`. Decouples the service from implementation details of each persistence/client module.
- **Location:** `app/repositories/comparacao_repository.py`
- **Contains:** `buscar_status_farmacias()`, `salvar_comparacao()`, `salvar_status_farmacias()`, `buscar_todos_consolidados()`, `buscar_historico_por_associacao()`, `buscar_ultima_atualizacao()`, `buscar_por_codigo()`
- **Depends on:** `app/local_db.py`, `app/clients/business_connect.py`, `app/clients/coletor_bi.py`
- **Used by:** `app/services/comparacao_service.py`, `app/routers/comparar.py`

### Client Layer
- **Purpose:** HTTP clients for external third-party services (not databases).
- **Location:** `app/clients/business_connect.py`, `app/clients/coletor_bi.py`
- **Contains:**
  - `business_connect.py`: `get_bearer_token()`, `get_status_farmacia()`, `buscar_status_farmacias()`
  - `coletor_bi.py`: `buscar_por_codigo()`, `buscar_por_associacao()`
- **Depends on:** `requests` library, environment variables
- **Used by:** `app/repositories/comparacao_repository.py`

### Mapper Layer
- **Purpose:** Pure translation functions from domain models → Pydantic response schemas. Zero business logic.
- **Location:** `app/mappers/comparacao_mapper.py`
- **Contains:** `montar_divergencia_response()`, `montar_status_farmacia_response()`, `montar_comparacao_response()`, `montar_resultado_consolidado()`
- **Depends on:** `app/schemas.py`, `app/utils.py`
- **Used by:** `app/services/comparacao_service.py`, `app/routers/comparar.py`

### Domain Model Layer
- **Purpose:** Internal domain objects (not tied to API serialization). Plain `@dataclass`.
- **Location:** `app/models/comparacao.py`
- **Contains:** `Divergencia` dataclass, `ResultadoComparacao` dataclass
- **Depends on:** nothing (pure Python)
- **Used by:** `app/services/comparacao_service.py`

### Infrastructure Layer
- **Purpose:** Raw database connection management.
- **Location:** `app/database.py` (Redshift), `app/local_db.py` (PostgreSQL local)
- **Contains:**
  - `database.py`: `get_connection()` context manager, `get_connection_config()`, `test_connection()`
  - `local_db.py`: `init_local_db()`, `salvar_comparacao()`, `buscar_todos_consolidados()`, `buscar_historico_por_associacao()`, `buscar_ultima_atualizacao()`, `salvar_status_farmacias()`
- **Depends on:** `redshift_connector`, `psycopg2`, `python-dotenv`
- **Used by:** `app/repositories/redshift_repository.py`, `app/repositories/comparacao_repository.py`

---

## Data Flow

### Active Comparison Request (`GET /comparar?associacao=80` or `POST /comparar`)

1. **Router** (`comparar.py`) receives the HTTP request with `associacao` parameter
2. **Router** calls `executar_comparacao(associacao)` in the service layer
3. **Service** calls `_comparar_resultados(associacao)`:
   - Calls `execute_gold_vendas(associacao)` → Redshift query on `associacao.vendas` JOIN `associacao.dimensao_cadastro_lojas`
   - Calls `execute_silver_stgn_dedup(associacao)` → Redshift query on `silver.cadcvend_staging_dedup`
   - Builds dicts keyed by `cod_farmacia` for each result set
   - Calls `execute_cadfilia_por_codigos(silver_only_codes)` → lookup for silver-only pharmacies missing cadastral data
   - Iterates over the union of all `cod_farmacia` keys; classifies each into one of three divergence types:
     - `apenas_gold_vendas` — cod exists only in Gold
     - `apenas_silver_stgn_dedup` — cod exists only in Silver
     - `data_diferente` — same cod but `ultima_venda` dates differ
   - Returns a `ResultadoComparacao` domain model
4. **Service** calls `buscar_status_farmacias(codigos)` → Business Connect API (parallel HTTP via `ThreadPoolExecutor`) for all pharmacies
5. **Service** calls `buscar_por_associacao(codigos)` → Coletor BI API (parallel HTTP via `ThreadPoolExecutor`) for all pharmacies
6. **Service** calls `salvar_comparacao(...)` → persists to local PostgreSQL (`comparacoes`, `resultados_gold_vendas`, `resultados_silver_stgn_dedup`, `farmacias` tables) via UPSERT
7. **Service** calls `salvar_status_farmacias(...)` → updates `coletor_novo`, `coletor_bi_ultima_data`, `coletor_bi_ultima_hora` columns in `farmacias` table
8. **Service** applies `camadas_atrasadas()` utility to each divergence to compute delay metadata
9. **Service** calls mapper functions to build Pydantic response objects
10. **Router** returns `ComparacaoResponse` JSON to client

### History Read Requests (`GET /historico`, `GET /historico/{associacao}`)

1. **Router** calls `buscar_todos_consolidados()` or `buscar_historico_por_associacao()` directly (no service layer — read-only path)
2. **Repository** delegates to `local_db.py` which executes SQL against local PostgreSQL
3. **Router** maps each row via `montar_resultado_consolidado()` → `ResultadoConsolidadoResponse`
4. Router returns list of consolidated records to client

### Health Check (`GET /health`)

1. `main.py` health endpoint calls `test_connection()` from `app/database.py`
2. Opens a real Redshift connection, executes `SELECT 1`, returns status dict

---

## Key Design Decisions

### Dual Database Architecture
- **Amazon Redshift** is the source-of-truth data warehouse (read-only from this API's perspective). Accessed via `redshift-connector`.
- **Local PostgreSQL** acts as a result cache / audit store. Stores the latest comparison run per pharmacy, with append-only history in `comparacoes`. Accessed via `psycopg2`.
- The API always re-queries Redshift on each `/comparar` call; local PostgreSQL serves historical reads.

### UPSERT Strategy for Local Persistence
`local_db.salvar_comparacao()` uses PostgreSQL `ON CONFLICT ... DO UPDATE` to maintain a current-state snapshot in `farmacias` and source tables, while `comparacoes` is append-only (each run creates a new row). Pharmacies that disappear from new results are explicitly deleted.

### Facade Repository Pattern
`app/repositories/comparacao_repository.py` is a thin delegation facade — every function is a one-liner forwarding to `local_db.py` or `clients/`. This decouples the service layer from the actual implementation modules and makes the service's imports declarative.

### Parallel External API Calls
Both `business_connect.buscar_status_farmacias()` and `coletor_bi.buscar_por_associacao()` use `ThreadPoolExecutor(max_workers=10)` to query all pharmacies concurrently. Business Connect obtains a Bearer token once and reuses it across all parallel requests.

### Graceful Degradation for External Services
In `executar_comparacao()`, both Business Connect and Coletor BI calls are wrapped in `try/except`. On failure, the service continues with a fallback dict (`"Indisponível"` per pharmacy) and logs a warning — the comparison result is still returned to the client.

### Domain Model ↔ Schema Separation
Internal logic uses `Divergencia` and `ResultadoComparacao` dataclasses (no Pydantic). Pydantic schemas (`ComparacaoResponse`, `DivergenciaResponse`, etc.) are only used at the API boundary, created by mapper functions. This prevents Pydantic validation overhead inside the comparison loop.

### CNPJ Sanitization
`local_db._sanitize_cnpj()` strips punctuation from CNPJs before persistence, ensuring consistent storage regardless of input formatting.

---

## Comparison Logic End-to-End

The core comparison in `_comparar_resultados()`:

1. Each query uses `ROW_NUMBER() OVER (PARTITION BY cod_farmacia ORDER BY dat_emissao DESC)` — this window function selects only the most recent sale per pharmacy.
2. Results are indexed into Python dicts `{cod_farmacia: row}`.
3. A union set of all `cod_farmacia` values is computed.
4. For each pharmacy code, one of three conditions is checked:
   - Only in `gold_by_farmacia` → `tipo_divergencia = "apenas_gold_vendas"`
   - Only in `silver_by_farmacia` → `tipo_divergencia = "apenas_silver_stgn_dedup"`
   - In both, but `str(ultima_venda_gold) != str(ultima_venda_silver)` → `tipo_divergencia = "data_diferente"`
   - In both with matching date → **not a divergence** (not included in output)
5. Pharmacies only in Silver lack cadastral data (name, CNPJ, etc.), so `execute_cadfilia_por_codigos()` is called as a supplemental lookup against `associacao.dimensao_cadastro_lojas` (primary) with gap-fill from `silver.cadfilia_staging_dedup`.

---

## Error Handling

**Strategy:** Layered try/except with controlled degradation.

- **Router layer:** All service calls wrapped in `try/except Exception` → raises `HTTPException(503)` with descriptive message
- **Service layer:** Business Connect and Coletor BI calls individually wrapped → degraded to `"Indisponível"` fallback; persistence errors logged as warnings (non-critical, comparison still returned)
- **Client layer:** Each individual pharmacy HTTP request in `ThreadPoolExecutor` is wrapped — individual failures don't abort the batch
- **Health check:** `test_connection()` catches `ValueError` (config) and generic `Exception` (network) separately, returning structured status dict

---

## Cross-Cutting Concerns

**Logging:** Python `logging` module with `logging.getLogger(__name__)` in every module. Configured in `main.py` with timestamped INFO-level format. Service layer logs timing (using `time.perf_counter()`) for each major step with emoji prefixes for readability.

**Validation:** Input validated at HTTP boundary via Pydantic schemas (`ComparacaoRequest`). Internal data is plain dicts/dataclasses — no validation mid-pipeline.

**Authentication (external):** Business Connect uses Bearer token (JWT), obtained fresh per comparison run via `POST /v1/auth`. Coletor BI uses HTTP Basic Auth (`COLETOR_USERNAME` / `COLETOR_PASSWORD`).

**CORS:** Configured in `main.py` via `CORSMiddleware`. Origins controlled by `CORS_ORIGINS` env var (defaults to `"*"`).

**Startup:** `lifespan` async context manager in `main.py` calls `init_local_db()` on startup to ensure PostgreSQL schema exists (CREATE TABLE IF NOT EXISTS + inline migrations).

---

*Architecture analysis: 2025-01-31*

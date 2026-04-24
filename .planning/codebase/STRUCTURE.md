# Codebase Structure

**Analysis Date:** 2025-01-31

## Directory Layout

```
Validacao-BackEnd/
├── app/                            # All application source code
│   ├── main.py                     # FastAPI app factory, CORS, lifespan, root/health endpoints
│   ├── database.py                 # Amazon Redshift connection management
│   ├── local_db.py                 # Local PostgreSQL persistence (schema + CRUD)
│   ├── schemas.py                  # Pydantic request/response models (API boundary)
│   ├── utils.py                    # Shared domain utility functions
│   ├── models/
│   │   └── comparacao.py           # Internal domain dataclasses (not API schemas)
│   ├── routers/
│   │   └── comparar.py             # All HTTP endpoints
│   ├── services/
│   │   └── comparacao_service.py   # Core business logic orchestration
│   ├── repositories/
│   │   ├── redshift_repository.py  # SQL queries against Redshift
│   │   └── comparacao_repository.py # Facade over local_db + clients
│   ├── mappers/
│   │   └── comparacao_mapper.py    # Domain model → Pydantic schema translation
│   └── clients/
│       ├── business_connect.py     # Business Connect REST API client
│       └── coletor_bi.py           # Coletor BI REST API client
├── requirements.txt                # Python dependencies
├── .env.example                    # Environment variable template (no secrets)
├── README.md                       # Project documentation
└── FRONTEND_INTEGRATION.md         # Frontend consumption guide
```

---

## Directory Purposes

### `app/` — Application Root
- **Purpose:** All Python application code. No subdirectory nesting beyond one level.
- **Key files:** `main.py` (entry point), `database.py`, `local_db.py`, `schemas.py`, `utils.py`

### `app/models/`
- **Purpose:** Internal domain model definitions. These are `@dataclass` objects used within the service layer — never serialized directly to JSON.
- **Key files:** `comparacao.py`
- **Note:** Do NOT add Pydantic models here. Models here have no knowledge of HTTP or database schema.

### `app/routers/`
- **Purpose:** FastAPI `APIRouter` definitions. One router per resource group.
- **Key files:** `comparar.py` — contains all current endpoints
- **Pattern:** Thin handlers; all logic delegated to services or repositories

### `app/services/`
- **Purpose:** Business logic and orchestration. Coordinates multiple repositories and clients.
- **Key files:** `comparacao_service.py`
- **Pattern:** Functions (not classes); each public function is a use case

### `app/repositories/`
- **Purpose:** Data access. Two types co-exist:
  - `redshift_repository.py` — direct SQL execution against Redshift
  - `comparacao_repository.py` — facade that delegates to `local_db.py` and `clients/`
- **Pattern:** Returns raw `list[dict]` or `dict`; no domain model construction

### `app/mappers/`
- **Purpose:** Pure translation from domain objects/dicts → Pydantic schemas. No I/O, no logic.
- **Key files:** `comparacao_mapper.py`
- **Pattern:** One mapper function per schema type

### `app/clients/`
- **Purpose:** HTTP clients for external third-party services.
- **Key files:** `business_connect.py`, `coletor_bi.py`
- **Pattern:** Each client module has a `buscar_*` public function that handles auth, parallel requests, and per-item error handling

---

## Key File Locations

### Entry Points
- `app/main.py`: Application factory. Creates `FastAPI` instance, configures CORS and lifespan, registers router. Run with `uvicorn app.main:app`.
- `app/routers/comparar.py`: All HTTP routes. Registered into `main.py` via `app.include_router(comparar.router)`.

### Configuration & Infrastructure
- `app/database.py`: `get_connection()` — the single source of Redshift connections. Import this in any module needing Redshift access.
- `app/local_db.py`: Schema definition (`init_local_db()`) and all PostgreSQL CRUD. Called at startup and by `comparacao_repository.py`.
- `.env.example`: Lists all required environment variables with placeholder values.

### Core Logic
- `app/services/comparacao_service.py`: `executar_comparacao(associacao: str) -> ComparacaoResponse` — the main use case function. `_comparar_resultados(associacao)` — private Redshift comparison returning `ResultadoComparacao`.
- `app/repositories/redshift_repository.py`: `QUERY_GOLD_VENDAS` and `QUERY_SILVER_STGN_DEDUP` SQL constants; `execute_gold_vendas()`, `execute_silver_stgn_dedup()`, `execute_cadfilia_por_codigos()`.
- `app/utils.py`: `camadas_atrasadas(data_gold, data_silver, coletor_novo)` — computes delay/missing-data metadata for divergences.

### Schema Definitions
- `app/schemas.py`: All Pydantic models: `ComparacaoRequest`, `ComparacaoResponse`, `DivergenciaResponse`, `FarmaciaStatusResponse`, `ResultadoConsolidadoResponse`, `AssociacaoResumoResponse`
- `app/models/comparacao.py`: `Divergencia` dataclass, `ResultadoComparacao` dataclass

---

## Module Responsibilities

### `app/main.py`
- Creates `FastAPI(title="Validacao-BackEnd", version="0.1.0")`
- Defines `lifespan` async context manager: calls `init_local_db()` on startup
- Configures `CORSMiddleware` using `CORS_ORIGINS` env var
- Registers `comparar.router`
- Defines `GET /` (info) and `GET /health` (calls `test_connection()`) endpoints

### `app/database.py`
- `get_connection_config()` — reads `REDSHIFT_HOST`, `REDSHIFT_USER`, `REDSHIFT_DATABASE`/`REDSHIFT_NAME`, `REDSHIFT_PASSWORD`/`REDSHIFT_PASS`, `REDSHIFT_PORT` from env
- `get_connection()` — `@contextmanager` yielding `redshift_connector.Connection`; always closes connection in `finally`
- `test_connection()` — opens connection, executes `SELECT 1`, returns `{"connected": bool, "message": str, "host": str}`

### `app/local_db.py`
- `init_local_db()` — creates 4 PostgreSQL tables (`comparacoes`, `resultados_gold_vendas`, `resultados_silver_stgn_dedup`, `farmacias`) and runs inline ALTER TABLE migrations
- `salvar_comparacao(associacao, resultados_gold_vendas, resultados_silver_stgn_dedup, divergencias)` → `int` — appends to `comparacoes`, UPSERTs source tables and `farmacias`; returns new `comparacao_id`
- `salvar_status_farmacias(comparacao_id, associacao, status_farmacias, coletor_bi)` — UPDATEs `farmacias` with coletor status
- `buscar_todos_consolidados()` → `list[dict]` — JOINs `farmacias` with `comparacoes`, all associations
- `buscar_historico_por_associacao(associacao)` → `list[dict]` — same JOIN filtered by `codigo_rede`
- `buscar_ultima_atualizacao()` → `Optional[str]` — `MAX(executado_em)` across all comparisons
- `get_local_connection()` — builds psycopg2 connection from `LOCAL_DB_URL`, `LOCAL_DB_USER`, `LOCAL_DB_PASS`
- `_sanitize_cnpj(cnpj)` — private helper: strips `.`, `-`, `/` from CNPJ strings

### `app/schemas.py`
| Class | Type | Purpose |
|---|---|---|
| `ComparacaoRequest` | Request | `POST /comparar` body — `associacao: str` |
| `ComparacaoResponse` | Response | Full comparison result with divergences and pharmacy status |
| `DivergenciaResponse` | Response item | One pharmacy divergence record |
| `FarmaciaStatusResponse` | Response item | Business Connect + Coletor BI status per pharmacy |
| `ResultadoConsolidadoResponse` | Response | Historical consolidated record from local DB |
| `AssociacaoResumoResponse` | Response | Summary stats for one association (defined but not used in current router) |

### `app/utils.py`
- `camadas_atrasadas(data_gold, data_silver, coletor_novo)` → `Tuple[Optional[list[str]], Optional[list[str]]]`
  - Returns `(camadas_atrasadas, camadas_sem_dados)`
  - Checks each data string against `date.today() - timedelta(days=1)` (D-1)
  - Includes `"API"` in `camadas_atrasadas` if `coletor_novo` starts with `"Pendente de envio no dia "` and that date is before D-1

### `app/models/comparacao.py`
- `Divergencia` — `@dataclass` with fields: `cod_farmacia`, `nome_farmacia`, `cnpj`, `sit_contrato`, `codigo_rede`, `ultima_venda_GoldVendas`, `ultima_hora_venda_GoldVendas`, `ultima_venda_SilverSTGN_Dedup`, `ultima_hora_venda_SilverSTGN_Dedup`, `tipo_divergencia`
- `ResultadoComparacao` — `@dataclass` with fields: `associacao`, `total_gold_vendas`, `total_silver_stgn_dedup`, `total_divergencias`, `divergencias: list[Divergencia]`, `comparacao_id`, `todas_farmacias: set`, `resultados_gold_vendas: list[dict]`, `resultados_silver_stgn_dedup: list[dict]`

### `app/routers/comparar.py`
| Route | Method | Handler | Description |
|---|---|---|---|
| `/comparar` | GET | `comparar()` | Runs comparison via query param `?associacao=` |
| `/comparar` | POST | `comparar_post()` | Same, but `associacao` in JSON body |
| `/historico` | GET | `listar_todas_farmacias()` | All pharmacies from local DB, all associations |
| `/historico/{associacao}` | GET | `historico_por_associacao()` | Pharmacies filtered by association |
| `/ultima-atualizacao` | GET | `ultima_atualizacao()` | Timestamp of most recent comparison |
| `/coletor/{codigo}` | GET | `coletor_codigo()` | Last sale data from Coletor BI for one pharmacy code |

### `app/services/comparacao_service.py`
- `_comparar_resultados(associacao)` → `ResultadoComparacao`
  - Calls `execute_gold_vendas` and `execute_silver_stgn_dedup` sequentially
  - Calls `execute_cadfilia_por_codigos` for silver-only codes and gold codes missing `sit_contrato`
  - Classifies each pharmacy into a `Divergencia` or skips it (no divergence)
- `executar_comparacao(associacao)` → `ComparacaoResponse`
  - Calls `_comparar_resultados`
  - Calls `buscar_status_farmacias` (Business Connect, parallel)
  - Calls `buscar_por_associacao` (Coletor BI, parallel)
  - Calls `salvar_comparacao` + `salvar_status_farmacias` (local PostgreSQL persistence)
  - Applies `camadas_atrasadas` to each divergence
  - Calls mapper functions to build final response

### `app/repositories/redshift_repository.py`
- `QUERY_GOLD_VENDAS` — SQL constant: `ROW_NUMBER()` window on `associacao.vendas` JOIN `associacao.dimensao_cadastro_lojas`, partitioned by `cod_farmacia`, ordered by `dat_emissao DESC`
- `QUERY_SILVER_STGN_DEDUP` — SQL constant: same `ROW_NUMBER()` pattern on `silver.cadcvend_staging_dedup`
- `execute_gold_vendas(associacao)` → `list[dict]`: takes `associacao` twice (for `v.associacao = %s` and `d.codigo_rede = %s`)
- `execute_silver_stgn_dedup(associacao)` → `list[dict]`
- `execute_cadfilia_por_codigos(codigos)` → `dict[str, dict]`: queries `associacao.dimensao_cadastro_lojas` as primary source, `silver.cadfilia_staging_dedup` as gap-fill for name/CNPJ/sit_contrato

### `app/repositories/comparacao_repository.py`
Pure delegation facade — every function is a one-liner forwarding call:
- → `app/clients/business_connect.buscar_status_farmacias`
- → `app/local_db.salvar_comparacao`
- → `app/local_db.salvar_status_farmacias`
- → `app/local_db.buscar_todos_consolidados`
- → `app/local_db.buscar_historico_por_associacao`
- → `app/local_db.buscar_ultima_atualizacao`
- → `app/clients/coletor_bi.buscar_por_codigo`

### `app/mappers/comparacao_mapper.py`
- `montar_divergencia_response(d: Divergencia, camadas_atrasadas, camadas_sem_dados)` → `DivergenciaResponse`
- `montar_status_farmacia_response(cod, status, coletor_bi)` → `FarmaciaStatusResponse`
- `montar_comparacao_response(resultado, divergencias, status_farmacias)` → `ComparacaoResponse`
- `montar_resultado_consolidado(row: dict)` → `ResultadoConsolidadoResponse`: calls `camadas_atrasadas()` utility and maps local DB row → schema

### `app/clients/business_connect.py`
- `get_bearer_token()` → `str`: `POST https://business-connect.triercloud.com.br/v1/auth` with `BC_USERNAME`/`BC_PASSWORD`; tries `access`, `access_token`, `token`, `accessToken` response keys
- `get_status_farmacia(cod_farmacia, token)` → `str`: `GET /v1/migration/pharmacy/{cod}/status`; returns `"Pendente de envio no dia {date}"` if `table_name=="cadcvend"` found, `"OK, sem registro"` otherwise
- `buscar_status_farmacias(codigos)` → `dict[str, str]`: authenticates once, parallel-queries all pharmacies via `ThreadPoolExecutor(max_workers=10)`

### `app/clients/coletor_bi.py`
- `buscar_por_codigo(codigo)` → `dict`: `GET {COLETOR_URL}/{codigo}-dados_vendas` with Basic Auth; returns `{"farmacia": str, "ultima_data": str|None, "ultima_hora": str|None}`; finds latest entry by `max()` on `dat_emissao + hor_emissao`
- `buscar_por_associacao(codigos)` → `dict[str, dict]`: parallel-queries all pharmacies via `ThreadPoolExecutor(max_workers=10)`

---

## Import Dependency Graph

```
main.py
  ├── app.database (test_connection)
  ├── app.local_db (init_local_db)
  └── app.routers.comparar

routers/comparar.py
  ├── app.repositories.comparacao_repository
  ├── app.mappers.comparacao_mapper
  ├── app.schemas
  └── app.services.comparacao_service

services/comparacao_service.py
  ├── app.clients.coletor_bi
  ├── app.models.comparacao
  ├── app.repositories.comparacao_repository
  ├── app.repositories.redshift_repository
  ├── app.mappers.comparacao_mapper
  ├── app.schemas
  └── app.utils

repositories/redshift_repository.py
  └── app.database

repositories/comparacao_repository.py
  ├── app.clients.business_connect
  ├── app.clients.coletor_bi
  └── app.local_db

mappers/comparacao_mapper.py
  ├── app.schemas
  └── app.utils

clients/business_connect.py
  └── requests (external)

clients/coletor_bi.py
  └── requests (external)

models/comparacao.py
  └── (none — pure Python dataclasses)

utils.py
  └── (none — stdlib only)

database.py
  ├── redshift_connector (external)
  └── python-dotenv (external)

local_db.py
  └── psycopg2 (external)

schemas.py
  └── pydantic (external)
```

---

## Naming Conventions

**Files:**
- Modules use `snake_case`: `comparacao_service.py`, `redshift_repository.py`, `coletor_bi.py`
- Names describe the layer role: `*_service.py`, `*_repository.py`, `*_mapper.py`

**Functions:**
- Redshift queries: `execute_*` prefix (`execute_gold_vendas`, `execute_silver_stgn_dedup`)
- Persistence: `salvar_*` prefix (`salvar_comparacao`, `salvar_status_farmacias`)
- Reads: `buscar_*` prefix (`buscar_todos_consolidados`, `buscar_por_codigo`)
- Response builders: `montar_*` prefix (`montar_divergencia_response`, `montar_comparacao_response`)
- Private helpers: `_` prefix (`_comparar_resultados`, `_sanitize_cnpj`, `_buscar_status_farmacias`)

**Schema fields:**
- Source-specific fields use CamelCase suffix: `ultima_venda_GoldVendas`, `ultima_hora_venda_SilverSTGN_Dedup`
- Standard fields: `snake_case`

---

## Where to Add New Code

**New HTTP endpoint:**
- Add route function to `app/routers/comparar.py`
- If the endpoint requires new business logic, add a function to `app/services/comparacao_service.py`
- If it's a simple read from local DB, call the repository directly from the router (following the pattern in `listar_todas_farmacias`)

**New Redshift query:**
- Add SQL constant and `execute_*` function to `app/repositories/redshift_repository.py`
- Use `get_connection()` context manager from `app/database.py`

**New external API client:**
- Create `app/clients/{service_name}.py`
- Expose functions via `app/repositories/comparacao_repository.py` facade

**New response schema:**
- Add Pydantic class to `app/schemas.py`
- Add mapper function to `app/mappers/comparacao_mapper.py`

**New domain concept:**
- Add `@dataclass` to `app/models/comparacao.py` (or new file under `app/models/`)

**New local DB table:**
- Add `CREATE TABLE IF NOT EXISTS` to `init_local_db()` in `app/local_db.py`
- Add CRUD functions to `app/local_db.py`
- Expose via `app/repositories/comparacao_repository.py`

---

## Special Directories / Files

**`.env.example`:**
- Purpose: Documents all required environment variables with placeholder values
- Generated: No (manually maintained)
- Committed: Yes — safe because it contains no real secrets

**`FRONTEND_INTEGRATION.md`:**
- Purpose: Documents the API contract for frontend consumers
- Committed: Yes

**`.planning/codebase/`:**
- Purpose: Architecture analysis documents for AI-assisted development
- Generated: Yes (by codebase mapper)
- Committed: Yes

---

*Structure analysis: 2025-01-31*

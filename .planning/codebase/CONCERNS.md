# Codebase Concerns

**Analysis Date:** 2026-04-17

---

## Security Concerns

### CORS Wildcard Default in Production
- **Severity:** HIGH
- **Issue:** `app/main.py` lines 43–44 default `CORS_ORIGINS` to `"*"` when the env var is absent. The `.env.example` file also sets `CORS_ORIGINS=*`. Any browser-based client from any origin can call all endpoints, including `/comparar` which triggers expensive Redshift queries.
- **Files:** `app/main.py:43-44`, `.env.example:17`
- **Fix:** Change the default to `""` (empty/deny-all) and require explicit configuration. Document in deployment README.

### No Authentication or Authorization on Any Endpoint
- **Severity:** HIGH
- **Issue:** All API endpoints — including `POST /comparar` (triggers Redshift + 2 external API calls), `GET /historico` (dumps full pharmacy database), and `GET /coletor/{codigo}` (proxies to Coletor BI) — are completely public with no API key, JWT, session, or IP allowlist protection.
- **Context:** `PROJECT.md` notes "Autenticação/autorização na API — não requerido na v1", but the API is now significantly more complex than v1 scope (external APIs, persistent local DB, sensitive business data).
- **Files:** `app/routers/comparar.py` (all routes), `app/main.py`
- **Fix:** Add a static API key header check via FastAPI `Depends`, or integrate an OAuth2 bearer token scheme. At minimum, implement IP allowlist middleware for internal-only APIs.

### Internal Error Class Names Leaked in HTTP Responses
- **Severity:** MEDIUM
- **Issue:** All error handlers in `app/routers/comparar.py` include `type(e).__name__` in the HTTP `detail` field (lines 31, 42, 52, 64, 72, 83, 93). This exposes internal Python class names (`psycopg2.OperationalError`, `ValueError`, etc.) to API consumers, aiding reconnaissance.
- **Files:** `app/routers/comparar.py:31,42,52,64,72,83,93`
- **Fix:** Log the full exception internally (already done) and return a generic message to the client: `"Erro interno. Consulte os logs para detalhes."`.

### Hardcoded Base URL for External Service
- **Severity:** MEDIUM
- **Issue:** `_BC_BASE_URL = "https://business-connect.triercloud.com.br/v1"` is hardcoded in `app/clients/business_connect.py:11`. If the Business Connect endpoint changes or a staging environment is needed, a code change is required instead of a config change.
- **Files:** `app/clients/business_connect.py:11`
- **Fix:** Move to environment variable `BC_BASE_URL` with the current value as default.

### `GET /health` Exposes Database Connectivity State Publicly
- **Severity:** LOW
- **Issue:** `GET /health` returns whether the Redshift connection succeeds and the (masked) hostname. While the host is partially masked, the endpoint is unauthenticated and reveals infrastructure status to anyone.
- **Files:** `app/main.py:67-83`, `app/database.py:67-109`
- **Fix:** Protect `/health` with a secret header or restrict to internal network. Alternatively, return only `{"status": "ok"}` without database details on public-facing deployments.

### Default Credentials in `.env.example`
- **Severity:** LOW
- **Issue:** `.env.example` line 12 contains `LOCAL_DB_URL=postgresql://root:root@localhost:5432/valida`. The `root:root` credential is a weak default that developers may copy verbatim into their `.env` without changing.
- **Files:** `.env.example:12`
- **Fix:** Replace with a placeholder like `postgresql://DB_USER:DB_PASSWORD@localhost:5432/valida` to force conscious credential selection.

---

## Reliability Concerns

### Local PostgreSQL Connections Are Never Explicitly Closed (Connection Leak)
- **Severity:** HIGH
- **Issue:** `app/local_db.py` uses `with get_local_connection() as conn:` throughout. However, `psycopg2.connect()` used as a context manager manages **transactions** (commit/rollback), NOT connection lifecycle. The connection is never `.close()`d — it leaks and relies on garbage collection.
  - Affected functions: `init_local_db`, `salvar_comparacao`, `buscar_ultima_atualizacao`, `buscar_todos_consolidados`, `buscar_historico_por_associacao`, `salvar_status_farmacias`
- **Files:** `app/local_db.py:28,105,127,240,249,277,306`
- **Fix:** Change `get_local_connection()` to a `@contextmanager` that calls `conn.close()` in a `finally` block:
  ```python
  @contextmanager
  def get_local_connection():
      conn = psycopg2.connect(...)
      try:
          yield conn
      finally:
          conn.close()
  ```

### No Connection Pooling (Redshift or PostgreSQL)
- **Severity:** HIGH
- **Issue:** `app/database.py:get_connection()` opens a new Redshift connection on every call. `app/local_db.py:get_local_connection()` opens a new PostgreSQL connection on every call. A single `/comparar` request triggers at minimum 3 Redshift connections (`execute_gold_vendas`, `execute_silver_stgn_dedup`, `execute_cadfilia_por_codigos`) plus multiple local DB connections. Under concurrent load, this is both slow and resource-exhausting.
- **Files:** `app/database.py:47-64`, `app/local_db.py:17-25`
- **Fix:** Use `psycopg2.pool.ThreadedConnectionPool` for local DB. For Redshift, investigate if `redshift_connector` supports pooling or introduce a manual pool. Alternatively, use SQLAlchemy's connection pool abstraction over both.

### `datetime.strptime` Called Outside Try/Except in `coletor_bi.py`
- **Severity:** MEDIUM
- **Issue:** In `app/clients/coletor_bi.py:66-69`, `datetime.strptime(f"{x['dat_emissao']} {x['hor_emissao']}", "%Y-%m-%d %H:%M:%S")` is called outside the `try/except` block that protects the `requests.get()` call. If `dat_emissao` or `hor_emissao` contain an unexpected format, a `ValueError` is raised, propagates through `ThreadPoolExecutor`, and is caught only at the `future.result()` level in `buscar_por_associacao` — silently swallowing the parsing error as a warning with no indication of which data was malformed.
- **Files:** `app/clients/coletor_bi.py:50-74`
- **Fix:** Wrap the `max()` + `datetime.strptime` call in its own try/except and return `None` dates with a warning log on parse failure.

### No Retry Logic on External API Calls
- **Severity:** MEDIUM
- **Issue:** Neither `app/clients/business_connect.py` nor `app/clients/coletor_bi.py` implement any retry logic. Transient network errors, rate limits (HTTP 429), or momentary 5xx responses immediately result in fallback values (`"OK, sem registro"` or `None` dates) with no retry attempt.
- **Files:** `app/clients/business_connect.py:28-46`, `app/clients/coletor_bi.py:22-34`
- **Fix:** Add exponential backoff retry using `tenacity` library (e.g., 3 retries, 1s/2s/4s waits). Apply to both `requests.get` and `requests.post` calls.

### Inline Schema Migration Runs on Every Startup
- **Severity:** MEDIUM
- **Issue:** `app/local_db.py:init_local_db()` runs `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` and `CREATE UNIQUE INDEX IF NOT EXISTS` statements on every application startup (lines 88–101). While `IF NOT EXISTS` is safe, this creates startup latency and risk: if a future migration breaks (e.g., changing a column type), the application will fail to start entirely.
- **Files:** `app/local_db.py:28-102`
- **Fix:** Replace with a proper migration tool (Alembic). Store migration state in a `schema_versions` table.

### `GET /comparar` Is a Synchronous Blocking Route
- **Severity:** MEDIUM
- **Issue:** `app/routers/comparar.py:21` defines `def comparar(...)` (not `async def`). FastAPI runs synchronous route handlers in a thread pool executor. A long-running comparison (Redshift queries + 2 external API fan-outs at 10 threads each) blocks one thread for potentially 30–60 seconds. Under concurrent requests, the thread pool can be exhausted.
- **Files:** `app/routers/comparar.py:21-32`, `app/services/comparacao_service.py`
- **Fix:** Make `comparar` and `comparar_post` `async def` and run blocking I/O via `asyncio.to_thread()`, or use background tasks to decouple the heavy computation from the HTTP response.

---

## Scalability Concerns

### `GET /historico` Returns All Records Without Pagination
- **Severity:** HIGH
- **Issue:** `app/routers/comparar.py:45-56` calls `buscar_todos_consolidados()` which issues a `SELECT ... FROM farmacias ...` with no `LIMIT`. `app/local_db.py:249-274` fetches the full result set and maps each row to a Pydantic model. As more associations are compared over time, this payload grows unboundedly.
- **Files:** `app/routers/comparar.py:45-56`, `app/local_db.py:249-274`
- **Fix:** Add `limit: int = 1000` and `offset: int = 0` query parameters to both the router and the repository query. Return total count alongside results.

### Unbounded `IN (...)` Clause in `execute_cadfilia_por_codigos`
- **Severity:** MEDIUM
- **Issue:** `app/repositories/redshift_repository.py:107` generates `",".join(["%s"] * len(codigos))` without any upper bound. If an association has 500+ pharmacies, a single `IN (...)` clause with 500 parameters is sent to Redshift. Large IN clauses degrade query planning performance and may hit Redshift's parameter limits.
- **Files:** `app/repositories/redshift_repository.py:107,160`
- **Fix:** Batch into chunks of 100–200 codes, run multiple queries, and merge results.

### ThreadPoolExecutor Not Bounded by Association Size
- **Severity:** MEDIUM
- **Issue:** Both `app/clients/business_connect.py:127` and `app/clients/coletor_bi.py:88` create `ThreadPoolExecutor(max_workers=10)` and submit one future per pharmacy code. For a large association (e.g., 300 pharmacies), 300 futures are submitted to a 10-worker pool. All 300 `requests.Session` objects are created before any futures complete, consuming memory proportional to association size.
- **Files:** `app/clients/business_connect.py:127`, `app/clients/coletor_bi.py:88`
- **Fix:** Chunk `codigos` into batches of 50 and process batch-by-batch, or use `asyncio` with `aiohttp` and a semaphore to control concurrency.

### No Caching of Redshift Results
- **Severity:** MEDIUM
- **Issue:** Every call to `GET /comparar` or `POST /comparar` runs fresh Redshift queries and calls both external APIs, regardless of how recently the same association was queried. The local database is used for persistence but the response is always recomputed from scratch.
- **Files:** `app/services/comparacao_service.py:143`
- **Fix:** After saving to local DB, serve `GET /historico/{associacao}` from the local DB instead of re-running Redshift queries. Add a `max_age` parameter so clients can request a fresh comparison only when needed.

---

## Maintainability Concerns

### `comparacao_repository.py` Is a Pure Pass-Through Layer
- **Severity:** MEDIUM
- **Issue:** `app/repositories/comparacao_repository.py` contains 7 functions, every one of which is a one-liner that calls a `_`-prefixed import from `app/local_db` or `app/clients`. It adds no logic, no transformation, and no error handling. The layer exists for architectural symmetry but creates indirection that makes tracing a call harder.
- **Files:** `app/repositories/comparacao_repository.py` (all 37 lines)
- **Fix:** Either remove this file and import directly from `local_db` and `clients` in the service, or justify the layer by adding caching, error normalization, or retry logic here.

### Type Mismatch: `cod_farmacia` Is `int` in `DivergenciaResponse` but `str` Everywhere Else
- **Severity:** MEDIUM
- **Issue:** `app/schemas.py:42` declares `cod_farmacia: int` in `DivergenciaResponse`, but `ResultadoConsolidadoResponse` (line 98), `FarmaciaStatusResponse` (line 80), and all service/repository code treat `cod_farmacia` as `str`. The `Divergencia` dataclass in `app/models/comparacao.py:9` also uses `str`. Pydantic will attempt to coerce string codes to `int`, which will fail for codes like `"00559"` or alphanumeric codes.
- **Files:** `app/schemas.py:42`, `app/models/comparacao.py:9`, `app/repositories/redshift_repository.py:84`
- **Fix:** Change `DivergenciaResponse.cod_farmacia` to `str` to match the rest of the codebase.

### `associacao` Parameter Implicitly Assumed Equal to `codigo_rede`
- **Severity:** MEDIUM
- **Issue:** `app/repositories/redshift_repository.py:82` calls `cursor.execute(QUERY_GOLD_VENDAS, (associacao, associacao))` — passing the same value for both `v.associacao = %s` and `d.codigo_rede = %s`. This hardcodes the assumption that `associacao == codigo_rede`, which is not documented or validated. If they ever differ, results will be silently incorrect.
- **Files:** `app/repositories/redshift_repository.py:82`
- **Fix:** Accept `codigo_rede` as a separate parameter or add an explicit assertion/comment documenting the invariant.

### `Divergencia` Dataclass Manually Serialized to Dict in Service
- **Severity:** LOW
- **Issue:** `app/services/comparacao_service.py:186-200` manually constructs a dict from `Divergencia` dataclass fields instead of using `dataclasses.asdict()`. This is fragile: if a new field is added to `Divergencia`, the dict construction in the service must also be updated manually.
- **Files:** `app/services/comparacao_service.py:186-200`, `app/models/comparacao.py`
- **Fix:** Replace with `from dataclasses import asdict` and `[asdict(d) for d in resultado.divergencias]`.

### Inconsistent Typo: `"Indisponivel"` vs `"Indisponível"`
- **Severity:** LOW
- **Issue:** In `app/services/comparacao_service.py:182`, when Coletor BI is unavailable, the fallback dict is set to `{cod: "Indisponivel"}` (missing accent). The Business Connect fallback on line 165 uses `"Indisponível"` (correct accent). The `FarmaciaStatusResponse` schema documents `"Indisponível"`. The frontend receives inconsistent values depending on which service failed.
- **Files:** `app/services/comparacao_service.py:165,182`
- **Fix:** Replace `"Indisponivel"` with `"Indisponível"` on line 182.

### `buscar_historico_por_associacao` Naming Is Misleading
- **Severity:** LOW
- **Issue:** `app/local_db.py:277` is named `buscar_historico_por_associacao` and `app/routers/comparar.py:58` exposes it as `GET /historico/{associacao}`, but the function returns **current state** (not historical time series). Its own docstring says "Busca o estado atual de todas as farmácias de uma associação." The `comparacoes` table IS append-only (true history), but none of it is exposed.
- **Files:** `app/local_db.py:277`, `app/routers/comparar.py:58`
- **Fix:** Rename to `buscar_estado_atual_por_associacao` and `GET /estado/{associacao}`, or implement actual history exposure.

### `coletor_bi.py` Lacks Module Docstring
- **Severity:** LOW
- **Issue:** `app/clients/coletor_bi.py` has no module-level docstring, unlike all other modules in the project. It also uses module-level global constants (`COLETOR_URL`, `COLETOR_USERNAME`, `COLETOR_PASSWORD`) instead of reading them inside functions, which means the env vars must be set before module import.
- **Files:** `app/clients/coletor_bi.py:1-16`
- **Fix:** Add module docstring and move `os.getenv(...)` calls inside `buscar_por_codigo()` so the module is safe to import in any environment.

### `REDSHIFT_SCHEMA` Env Var Defined but Never Used
- **Severity:** LOW
- **Issue:** `.env.example` defines `REDSHIFT_SCHEMA=public` (line 9), but `app/database.py:get_connection_config()` never reads or uses `REDSHIFT_SCHEMA`. This is either dead configuration or a missing implementation (schema search path should be set on the connection).
- **Files:** `.env.example:9`, `app/database.py:13-44`
- **Fix:** Either remove `REDSHIFT_SCHEMA` from `.env.example`, or apply it via `cursor.execute(f"SET search_path TO {schema}")` after connecting.

---

## Technical Debt

### No Test Suite Whatsoever
- **Severity:** HIGH
- **Issue:** There are zero test files in the project (`tests/` directory does not exist). No unit tests, integration tests, or contract tests. The comparison logic in `app/services/comparacao_service.py` and the mapper logic in `app/mappers/comparacao_mapper.py` are untested. Schema changes (e.g., adding a new divergence type) cannot be safely validated.
- **Fix approach:** Add `pytest` + `pytest-asyncio` + `httpx` (for FastAPI test client). Priority test targets:
  1. `app/utils.py:camadas_atrasadas` — pure function, trivial to unit test
  2. `app/services/comparacao_service.py:_comparar_resultados` — mock Redshift calls, test divergence detection logic
  3. `app/routers/comparar.py` — integration tests with mocked service

### `GET /comparar` Uses GET Semantics for a State-Mutating Operation
- **Severity:** MEDIUM
- **Issue:** `GET /comparar?associacao=80` runs Redshift queries, calls 2 external APIs, and **writes** to the local PostgreSQL database. GET requests should be idempotent and side-effect free. Browser prefetching, caching proxies, or API gateways may call GET endpoints automatically, triggering unintended comparisons and DB writes.
- **Files:** `app/routers/comparar.py:21-32`
- **Fix:** Remove `GET /comparar` or make it read-only (serve from local cache). Keep `POST /comparar` as the trigger for fresh comparisons.

### Scope Drift from Original Requirements
- **Severity:** LOW (informational)
- **Issue:** `PROJECT.md` explicitly lists "Persistência/histórico de comparações — fora do escopo inicial" and "Autenticação/autorização na API — não requerido na v1" as Out of Scope. The implementation includes a full persistence layer (`app/local_db.py`, 336 lines), a history endpoint (`GET /historico`), and 2 external API integrations not mentioned in requirements. This is not a problem in itself but means PROJECT.md no longer reflects reality and should be updated.
- **Files:** `.planning/PROJECT.md`
- **Fix:** Update PROJECT.md to reflect current scope, move implemented items from "Out of Scope" to validated requirements.

---

## Missing Critical Features

### No Input Validation on `associacao` Parameter
- **Severity:** MEDIUM
- **Issue:** The `associacao` parameter in `ComparacaoRequest` and `GET /comparar?associacao=` accepts any string of any length with no validation. A malformed or very long string is passed directly into Redshift queries (parameterized — not SQL injection risk) but could cause confusing Redshift errors or unexpectedly empty results with no meaningful error message.
- **Files:** `app/schemas.py:19-30`, `app/routers/comparar.py:21-43`
- **Fix:** Add `Field(..., min_length=1, max_length=20, pattern=r"^\d+$")` to `ComparacaoRequest.associacao` to enforce numeric-only codes.

### No Timeout on Redshift Queries
- **Severity:** MEDIUM
- **Issue:** `app/database.py:get_connection()` uses `redshift_connector.connect()` with no `connect_timeout` parameter and no query timeout. A slow or hung Redshift query will block the request indefinitely.
- **Files:** `app/database.py:59-60`
- **Fix:** Add `connect_timeout=10` to the connection config and set `cursor.execute("SET statement_timeout TO 30000")` (30s) after connecting.

### `/comparar` Has No Protection Against Concurrent Duplicate Requests
- **Severity:** LOW
- **Issue:** If the frontend double-clicks "Atualizar" or a slow network causes a retry, two concurrent `POST /comparar` for the same `associacao` will both trigger full Redshift queries simultaneously and attempt concurrent writes to the same `farmacias` rows. The UPSERT logic should handle this, but timing races between the `DELETE` and `INSERT` steps in `salvar_comparacao` could cause integrity errors.
- **Files:** `app/local_db.py:105-237`
- **Fix:** Add an advisory lock per `associacao` during the comparison operation, or use a simple in-memory lock dict at the service level.

---

## Test Coverage Gaps

### Divergence Detection Logic Is Untested
- **What's not tested:** The three divergence types (`data_diferente`, `apenas_gold_vendas`, `apenas_silver_stgn_dedup`) computed in `_comparar_resultados` have no tests. Edge cases (both dates `None`, one side empty list, string vs date comparison) are unverified.
- **Files:** `app/services/comparacao_service.py:28-140`
- **Risk:** Silent regressions if comparison logic is modified
- **Priority:** HIGH

### `camadas_atrasadas` Utility Is Untested
- **What's not tested:** The date threshold logic (D-1 boundary), the `"Pendente de envio no dia"` string parsing, and the None-handling paths in `app/utils.py:camadas_atrasadas`.
- **Files:** `app/utils.py`
- **Risk:** Incorrect "delayed" flags silently displayed on the dashboard
- **Priority:** HIGH

### Local DB Persistence Functions Are Untested
- **What's not tested:** UPSERT logic in `salvar_comparacao`, the delete-before-upsert patterns, and the `salvar_status_farmacias` update path in `app/local_db.py`.
- **Files:** `app/local_db.py:105-237,306-335`
- **Risk:** Data loss or silent corruption during association updates
- **Priority:** MEDIUM

### Mapper Functions Are Untested
- **What's not tested:** `montar_resultado_consolidado` in `app/mappers/comparacao_mapper.py` performs case-sensitive dict key access (`"ultima_venda_goldvendas"` lowercase) which depends on psycopg2's `RealDictCursor` returning lowercase keys. This could silently return `None` for all date fields if key casing changes.
- **Files:** `app/mappers/comparacao_mapper.py:49-73`
- **Risk:** All date/time fields could silently be `None` in API responses
- **Priority:** MEDIUM

---

*Concerns audit: 2026-04-17*

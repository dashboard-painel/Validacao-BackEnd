# External Integrations

**Analysis Date:** 2025-01-31

## APIs & External Services

### Business Connect API

- **Purpose:** Checks migration status of each pharmacy (`cod_farmacia`) in the Business Connect data pipeline. Determines whether pharmacy sales data has been submitted (`cadcvend` table).
- **Base URL:** `https://business-connect.triercloud.com.br/v1` (hardcoded in `app/clients/business_connect.py`, constant `_BC_BASE_URL`)
- **SDK/Client:** `requests` library (no dedicated SDK)
- **Implementation:** `app/clients/business_connect.py`
- **Auth:** Form POST to `/v1/auth` with fields `code` (maps to `BC_USERNAME`) and `password` (maps to `BC_PASSWORD`). Returns Bearer token under keys `access`, `access_token`, `token`, or `accessToken` (tries all four).
- **Endpoints used:**
  - `POST /v1/auth` — obtain Bearer token (called once per comparison run)
  - `GET /v1/migration/pharmacy/{cod_farmacia}/status` — per-pharmacy migration status; looks for record with `table_name == "cadcvend"`
- **Return values stored:** `"OK, sem registro"` | `"Pendente de envio no dia {data_upload_datalake}"` | `"Indisponível"` (on error)
- **Parallelism:** `ThreadPoolExecutor(max_workers=10)` — all pharmacy codes queried concurrently; token fetched once and shared across all threads
- **Error handling:** Auth failures raise `Exception` (propagated to service); per-pharmacy failures fall back to `"OK, sem registro"`; full service failure falls back to `"Indisponível"` for all pharmacies

### Coletor BI API

- **Purpose:** Fetches the most recent `dat_emissao` + `hor_emissao` from the legacy BI collector for each pharmacy. Used to populate `coletor_bi_ultima_data` / `coletor_bi_ultima_hora` fields.
- **Base URL:** `COLETOR_URL` env var (no hardcoded value)
- **SDK/Client:** `requests` library (no dedicated SDK)
- **Implementation:** `app/clients/coletor_bi.py`
- **Auth:** HTTP Basic Auth using `COLETOR_USERNAME` and `COLETOR_PASSWORD` env vars
- **Endpoint pattern:** `GET {COLETOR_URL}/{cod_farmacia}-dados_vendas`
- **Response parsing:** Expects a JSON array; selects the record with the latest `dat_emissao` + `hor_emissao` using `datetime.strptime("%Y-%m-%d %H:%M:%S")`
- **Return shape:** `{"farmacia": str, "ultima_data": str|None, "ultima_hora": str|None}`
- **Parallelism:** `ThreadPoolExecutor(max_workers=10)` — all pharmacy codes queried concurrently
- **Error handling:** Timeout or non-200 responses return `{"ultima_data": None, "ultima_hora": None}` with a `logger.warning`; timeout set to `30` seconds

---

## Data Storage

### Amazon Redshift (primary analytical source)

- **Purpose:** Source of truth for pharmacy sales data. Two schemas/tables are queried per comparison run.
- **Connection:** `app/database.py` — `get_connection()` context manager using `redshift_connector.connect()`
- **Client library:** `redshift-connector>=2.0.0`
- **Default port:** `5439`
- **Connection config:** Built from env vars in `get_connection_config()` — see Environment Variables section
- **Query execution:** Raw parameterized SQL with `%s` placeholders (psycopg2-style); cursors used directly
- **Tables queried:**

  | Table | Alias | Purpose |
  |-------|-------|---------|
  | `associacao.vendas` | GoldVendas | Last sale date per pharmacy in gold layer |
  | `associacao.dimensao_cadastro_lojas` | — | Pharmacy master data (name, CNPJ, `sit_contrato`, `codigo_rede`) |
  | `silver.cadcvend_staging_dedup` | SilverSTGN_Dedup | Last sale date per pharmacy in silver layer |
  | `silver.cadfilia_staging_dedup` | — | Fallback pharmacy name/CNPJ for silver-only pharmacies |

- **Query files:** `app/repositories/redshift_repository.py`
  - `QUERY_GOLD_VENDAS` — parameterized by `(associacao, associacao)` (used for both `v.associacao` filter and `d.codigo_rede` filter)
  - `QUERY_SILVER_STGN_DEDUP` — parameterized by `(associacao,)`
  - `execute_cadfilia_por_codigos(codigos)` — dynamic `IN (...)` with `%s` per element

### Local PostgreSQL (history/cache store)

- **Purpose:** Persists comparison results for fast reads on `/historico` endpoints, avoiding repeated Redshift queries.
- **Connection:** `app/local_db.py` — `get_local_connection()` using `psycopg2.connect(url)`
- **Client library:** `psycopg2-binary>=2.9.0`; reads use `RealDictCursor` for dict-style rows
- **Default URL:** `postgresql://root:root@localhost:5432/valida` (from `LOCAL_DB_URL`; supports `LOCAL_DB_USER` / `LOCAL_DB_PASS` for credential injection without modifying the URL)
- **Schema (auto-created on startup in `init_local_db()`):**

  | Table | Key | Description |
  |-------|-----|-------------|
  | `comparacoes` | `id SERIAL PK` | Append-only run history; one row per execution |
  | `resultados_gold_vendas` | `UNIQUE (codigo_rede, cod_farmacia)` | Last gold layer data per pharmacy per run |
  | `resultados_silver_stgn_dedup` | `UNIQUE (associacao, cod_farmacia)` | Last silver layer data per pharmacy per run |
  | `farmacias` | `UNIQUE (codigo_rede, cod_farmacia)` | Consolidated view: both sources + coletor status + divergence type |

- **Write strategy:** UPSERT (`ON CONFLICT ... DO UPDATE`) on stable pharmacy keys; `comparacoes` is append-only; old pharmacy records not in the new result set are deleted (DELETE before UPSERT)
- **Inline migrations:** `init_local_db()` runs `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` and `DROP COLUMN IF EXISTS` for schema evolution

---

## Authentication & Identity

**Redshift:**
- Username/password credentials stored in env vars (`REDSHIFT_USER`, `REDSHIFT_PASSWORD` or `REDSHIFT_PASS`)
- Direct connection — no IAM or token-based auth detected

**Business Connect:**
- Form-based login flow: `POST /v1/auth` → Bearer token
- Token is short-lived (fetched once per comparison run, not cached between requests)
- Env vars: `BC_USERNAME`, `BC_PASSWORD`

**Coletor BI:**
- HTTP Basic Auth on every request
- Env vars: `COLETOR_USERNAME`, `COLETOR_PASSWORD`

**No application-level auth on this API itself** — no authentication middleware, no API keys required for incoming requests.

---

## Data Flow

**Full comparison request (`GET /comparar?associacao=X` or `POST /comparar`):**

```
HTTP Request (associacao=X)
       │
       ├─ 1. Redshift: QUERY_GOLD_VENDAS(X, X)            → list[dict] (cod_farmacia, ultima_venda, …)
       ├─ 2. Redshift: QUERY_SILVER_STGN_DEDUP(X)         → list[dict] (cod_farmacia, ultima_venda, …)
       ├─ 3. Redshift: execute_cadfilia_por_codigos(…)     → dict (gap-fill for silver-only pharmacies)
       ├─ 4. Domain logic: compare by cod_farmacia         → list[Divergencia]
       ├─ 5. Business Connect (parallel, 10 threads)       → dict[cod → status string]
       ├─ 6. Coletor BI (parallel, 10 threads)             → dict[cod → {ultima_data, ultima_hora}]
       ├─ 7. PostgreSQL local: salvar_comparacao(…)        → comparacao_id (UPSERT)
       ├─ 8. PostgreSQL local: salvar_status_farmacias(…)  → update farmacias.coletor_novo/bi
       └─ 9. Mapper → ComparacaoResponse (JSON)
```

**History read (`GET /historico`):**
```
HTTP Request
       └─ PostgreSQL local: buscar_todos_consolidados()   → JOIN farmacias + comparacoes → JSON
```

---

## Environment Variable Configuration

All variables loaded via `python-dotenv` from `.env` (template at `.env.example`).

| Variable | Required | Default | Used In | Description |
|----------|----------|---------|---------|-------------|
| `REDSHIFT_HOST` | Yes | — | `app/database.py` | Redshift cluster endpoint |
| `REDSHIFT_PORT` | No | `5439` | `app/database.py` | Redshift port |
| `REDSHIFT_DATABASE` | Yes* | — | `app/database.py` | Database name (`REDSHIFT_NAME` alias accepted) |
| `REDSHIFT_USER` | Yes | — | `app/database.py` | Redshift username |
| `REDSHIFT_PASSWORD` | Yes* | — | `app/database.py` | Redshift password (`REDSHIFT_PASS` alias accepted) |
| `LOCAL_DB_URL` | Yes | — | `app/local_db.py` | PostgreSQL connection URL (strips `jdbc:` prefix if present) |
| `LOCAL_DB_USER` | No | — | `app/local_db.py` | Injects user into `LOCAL_DB_URL` if `@` not in URL |
| `LOCAL_DB_PASS` | No | — | `app/local_db.py` | Injects password into `LOCAL_DB_URL` if `@` not in URL |
| `BC_USERNAME` | Yes | — | `app/clients/business_connect.py` | Business Connect login code |
| `BC_PASSWORD` | Yes | — | `app/clients/business_connect.py` | Business Connect password |
| `COLETOR_URL` | Yes | — | `app/clients/coletor_bi.py` | Base URL for Coletor BI API |
| `COLETOR_USERNAME` | Yes | — | `app/clients/coletor_bi.py` | Coletor BI Basic Auth username |
| `COLETOR_PASSWORD` | Yes | — | `app/clients/coletor_bi.py` | Coletor BI Basic Auth password |
| `CORS_ORIGINS` | No | `*` | `app/main.py` | Comma-separated allowed origins or `*` |

*`REDSHIFT_DATABASE`/`REDSHIFT_NAME` and `REDSHIFT_PASSWORD`/`REDSHIFT_PASS` are alias pairs — either name works.

---

## API Endpoints Exposed

All endpoints registered in `app/routers/comparar.py`, mounted at root path in `app/main.py`.

| Method | Path | Input | Response Schema | Description |
|--------|------|-------|-----------------|-------------|
| `GET` | `/` | — | `dict` | API name/version/status |
| `GET` | `/health` | — | `dict` | Redshift connectivity check |
| `GET` | `/comparar` | `?associacao=` query param | `ComparacaoResponse` | Full comparison run |
| `POST` | `/comparar` | `ComparacaoRequest` JSON body | `ComparacaoResponse` | Full comparison run |
| `GET` | `/historico` | — | `list[ResultadoConsolidadoResponse]` | All pharmacies from local DB |
| `GET` | `/historico/{associacao}` | path param | `list[ResultadoConsolidadoResponse]` | Filtered by association |
| `GET` | `/ultima-atualizacao` | — | `{"atualizado_em": str\|null}` | Timestamp of latest run |
| `GET` | `/coletor/{codigo}` | path param | `{"data_hora_ultima_venda": ...}` | Single pharmacy Coletor BI lookup |

**Error codes:**
- `422` — Missing required parameter (`associacao`)
- `503` — Redshift/local DB connection failure
- `404` — Association not found (only `/historico/{associacao}`)

---

## Webhooks & Callbacks

**Incoming:** None  
**Outgoing:** None — all external calls are synchronous request/response within a single comparison run.

---

*Integration audit: 2025-01-31*

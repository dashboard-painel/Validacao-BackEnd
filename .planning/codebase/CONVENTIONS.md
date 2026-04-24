# Coding Conventions

**Analysis Date:** 2025-01-31

## Naming Patterns

### Files
- **snake_case** for all Python modules: `comparacao_service.py`, `redshift_repository.py`, `comparacao_mapper.py`
- File names are descriptive of their role: `database.py`, `local_db.py`, `schemas.py`, `utils.py`
- Routers named after the resource they expose: `comparar.py`
- Clients named after the external service: `business_connect.py`, `coletor_bi.py`

### Functions
- **snake_case** for all functions, with **Portuguese verb-noun pattern** matching business domain:
  - `executar_comparacao()` — `app/services/comparacao_service.py`
  - `salvar_comparacao()` — `app/local_db.py`
  - `buscar_ultima_atualizacao()` — `app/local_db.py`
  - `buscar_todos_consolidados()` — `app/local_db.py`
  - `buscar_historico_por_associacao()` — `app/local_db.py`
  - `montar_comparacao_response()` — `app/mappers/comparacao_mapper.py`
  - `montar_divergencia_response()` — `app/mappers/comparacao_mapper.py`
  - `camadas_atrasadas()` — `app/utils.py`
- Private/internal functions are prefixed with `_`:
  - `_comparar_resultados()` — `app/services/comparacao_service.py`
  - `_sanitize_cnpj()` — `app/local_db.py`
- Repository wrapper functions re-export local module functions with `_` prefix aliases:
  ```python
  # app/repositories/comparacao_repository.py
  from app.local_db import salvar_comparacao as _salvar_comparacao
  def salvar_comparacao(...): return _salvar_comparacao(...)
  ```

### Variables
- **snake_case** throughout: `resultados_gold_vendas`, `silver_by_farmacia`, `cod_farmacia`
- Lookup dict variables use pattern `{entity}_by_{key}`: `gold_by_farmacia`, `silver_by_farmacia`, `gold_by_cod`
- Timing variables use `t0`, `t_req`, `t_bc`, `t_auth`, `t_coletor` (short, with descriptive suffix)
- Set variables use descriptive plural nouns: `todas_farmacias`, `silver_only_codes`

### Classes (Pydantic Models & Dataclasses)
- **PascalCase** for all classes
- Pydantic schemas in `app/schemas.py` suffixed with `Response` or `Request`:
  - `ComparacaoRequest`, `ComparacaoResponse`
  - `DivergenciaResponse`, `FarmaciaStatusResponse`
  - `AssociacaoResumoResponse`, `ResultadoConsolidadoResponse`
- Domain model dataclasses in `app/models/comparacao.py` have no suffix:
  - `Divergencia`, `ResultadoComparacao`

### Constants
- **UPPER_SNAKE_CASE** for module-level constants:
  - `QUERY_GOLD_VENDAS`, `QUERY_SILVER_STGN_DEDUP` — `app/repositories/redshift_repository.py`
  - `_BC_BASE_URL` — `app/clients/business_connect.py` (private constant uses leading `_`)
  - `COLETOR_URL`, `COLETOR_USERNAME`, `COLETOR_PASSWORD` — `app/clients/coletor_bi.py`

---

## Language / Locale Conventions

The project uses **mixed Portuguese and English** naming with a clear pattern:

| Context | Language | Examples |
|---|---|---|
| Business domain identifiers | Portuguese | `associacao`, `cod_farmacia`, `divergencia`, `comparacao`, `farmacia` |
| Data layer / DB column names | Portuguese | `executado_em`, `ultima_venda`, `tipo_divergencia`, `nome_farmacia` |
| Technical infrastructure | English | `get_connection()`, `execute_gold_vendas()`, `router`, `lifespan` |
| Log messages | Mixed (Portuguese nouns, English verbs) | `"Erro ao executar comparação: %s"`, `"Comparação concluída em %.2fs"` |
| Docstrings | Portuguese | `"Retorna a data/hora da comparação mais recente..."` |
| Code comments | Portuguese | `"# Nova linha a cada rodada — histórico de execuções"` |
| API field descriptions (Pydantic `Field.description`) | Portuguese | `"Código da associação comparada"` |
| Divergence type strings (enum-like values) | Portuguese snake_case | `"data_diferente"`, `"apenas_gold_vendas"`, `"apenas_silver_stgn_dedup"` |

**Guideline:** New business-domain identifiers use Portuguese. Infrastructure/framework glue code uses English.

---

## Code Style

### Type Hints
- Used consistently on all function signatures, both parameters and return types:
  ```python
  # app/database.py
  def get_connection_config() -> dict:
  def get_connection() -> Generator[redshift_connector.Connection, None, None]:
  def test_connection() -> dict:

  # app/utils.py
  def camadas_atrasadas(
      data_gold: Optional[str],
      data_silver: Optional[str],
      coletor_novo: Optional[str],
  ) -> Tuple[Optional[list[str]], Optional[list[str]]]:
  ```
- Uses modern Python 3.10+ union syntax where applicable (`dict | None` in `app/local_db.py`, `app/repositories/comparacao_repository.py`)
- Uses `Optional[T]` and `list[T]` (lowercase list) interchangeably across files — no strict consistency between the two styles

### Docstrings
- Present on most public functions; **missing on some functions** (e.g., `montar_divergencia_response()` in `app/mappers/comparacao_mapper.py`, router handlers in `app/routers/comparar.py`, `buscar_por_associacao()` in `app/clients/coletor_bi.py`)
- Format: multi-line with `Args:` and `Returns:` sections for complex functions, single-line for simple ones
- Written in **Portuguese** with English technical terms mixed in
- Example full docstring pattern:
  ```python
  def execute_gold_vendas(associacao: str) -> list[dict]:
      """Executa a query na tabela associacao.vendas (GoldVendas) no Redshift.

      Retorna a última venda registrada por farmácia (cod_farmacia), sem filtro de data.

      Args:
          associacao: Código da associação para filtrar

      Returns:
          Lista de dicionários com chaves: cod_farmacia, nome_farmacia, cnpj,
          sit_contrato, codigo_rede, ultima_venda, ultima_hora_venda
      """
  ```

### Module-Level Docstrings
- Every module (`.py` file) starts with a module docstring in Portuguese:
  ```python
  """Módulo de queries SQL para execução no Redshift."""
  """Schemas Pydantic para validação e serialização de respostas da API."""
  """Router para endpoint de comparação de dados."""
  ```
- Exception: `app/clients/coletor_bi.py` and `app/repositories/comparacao_repository.py` have no module docstring

---

## Error Handling

### Pattern: Catch-and-Raise at Router, Catch-and-Fallback at Service
- **Routers** (`app/routers/comparar.py`) catch all exceptions and re-raise as `HTTPException` with status `503`:
  ```python
  except Exception as e:
      logger.error("Erro ao executar comparação: %s: %s", type(e).__name__, e)
      raise HTTPException(
          status_code=503,
          detail=f"Erro de conexão com o banco de dados. Tente novamente em alguns instantes. Detalhes: {type(e).__name__}",
      )
  ```
- **Services** (`app/services/comparacao_service.py`) use try/except with graceful fallback for non-critical calls (Business Connect, Coletor BI, persistence):
  ```python
  except Exception as e:
      logger.warning("Business Connect indisponível (%.2fs): %s: %s", ...)
      status_dict = {cod: "Indisponível" for cod in resultado.todas_farmacias}
  ```
- **Clients** (`app/clients/business_connect.py`, `app/clients/coletor_bi.py`) return safe defaults on failure instead of raising:
  ```python
  except requests.RequestException as e:
      logger.warning("Business Connect request falhou para farmácia %s: %s", cod_farmacia, e)
      return "OK, sem registro"
  ```
- **Database layer** (`app/database.py`) raises `ValueError` for missing config; other exceptions propagate naturally
- **`404 Not Found`** is used in `app/routers/comparar.py` for missing `associacao` history

### Error Message Format
- Always includes `type(e).__name__` to expose the exception class to consumers
- Log format: `"Descrição do erro: %s: %s", type(e).__name__, e`

---

## Logging

### Setup
- Configured once at application startup in `app/main.py`:
  ```python
  logging.basicConfig(
      level=logging.INFO,
      format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
      datefmt="%Y-%m-%d %H:%M:%S",
  )
  ```

### Logger Per Module
- Each module creates its own named logger with `logging.getLogger(__name__)`:
  ```python
  logger = logging.getLogger(__name__)
  ```
  Used in: `app/main.py`, `app/routers/comparar.py`, `app/services/comparacao_service.py`,
  `app/repositories/redshift_repository.py`, `app/clients/business_connect.py`, `app/clients/coletor_bi.py`

### Log Level Usage
- `logger.info()` — normal operation milestones, timing data, record counts
- `logger.warning()` — degraded but non-fatal: external service unavailable, fallbacks activated
- `logger.error()` — endpoint-level caught exceptions that result in HTTP error responses

### Emoji Prefixes in Log Messages
- Used consistently in `comparacao_service.py` and `redshift_repository.py` to visually distinguish phases:
  - `🔍` — Starting comparison
  - `⏳` — Waiting for async response
  - `✅` — Success
  - `🏁` — Completion summary
  - `🚀` — Final response ready
  - `📥` — Incoming request

### Timing Pattern
- All external calls (Redshift, Business Connect, Coletor BI) are timed with `time.perf_counter()`:
  ```python
  t0 = time.perf_counter()
  # ... call ...
  elapsed = time.perf_counter() - t0
  logger.info("✅ Redshift respondeu em %.2fs — %d registros retornados", elapsed, len(rows))
  ```

---

## Response / Schema Patterns

### Pydantic v2
- All schemas use Pydantic v2 (`pydantic>=2.0.0`)
- `model_config` dict replaces the deprecated inner `Config` class:
  ```python
  model_config = {
      "json_schema_extra": {
          "example": { ... }
      }
  }
  ```
- All fields use `Field(...)` for required fields or `Field(None, ...)` for optional, always with `description=`

### Field Naming in Schemas
- Business domain fields use Portuguese snake_case: `cod_farmacia`, `nome_farmacia`, `tipo_divergencia`
- Date/time fields follow the pattern `ultima_venda_{Source}` and `ultima_hora_venda_{Source}`:
  - `ultima_venda_GoldVendas`, `ultima_hora_venda_GoldVendas`
  - `ultima_venda_SilverSTGN_Dedup`, `ultima_hora_venda_SilverSTGN_Dedup`
  - Note: `{Source}` portion uses **PascalCase** embedded in a snake_case field name — a deliberate mixed convention to preserve source identity

### Response Shape Conventions
- Date fields are serialized as `Optional[str]` in format `"YYYY-MM-DD"` (not Python `date` objects)
- Time fields are serialized as `Optional[str]` in format `"HH:MM:SS"`
- List fields use `default_factory=list` when the field is always present but may be empty
- `comparacao_id` is `Optional[int]` because persistence may fail gracefully

---

## Configuration Patterns

### Environment Variables
- Loaded at module initialization via `load_dotenv()` — called in both `app/main.py` and `app/database.py` (redundant but safe)
- Access pattern: `os.getenv("VAR_NAME", "default_value")`
- Config validation is done eagerly in `get_connection_config()` (`app/database.py`), raising `ValueError` with a list of all missing variables at once
- Supports aliased variable names for backward compatibility:
  ```python
  database = os.getenv("REDSHIFT_DATABASE") or os.getenv("REDSHIFT_NAME")
  password = os.getenv("REDSHIFT_PASSWORD") or os.getenv("REDSHIFT_PASS")
  ```
- Template provided in `.env.example`; actual `.env` is gitignored

### Dependency Injection
- No DI framework — dependencies are imported directly at module level
- The repository layer (`app/repositories/comparacao_repository.py`) acts as a thin adapter, re-exporting functions from `app/local_db.py` and `app/clients/` to decouple the service from implementation details

### CORS
- Configured in `app/main.py` via env var `CORS_ORIGINS`
- Default value is `"*"` (wide-open for development)
- Production usage: comma-separated list of allowed origins

---

*Convention analysis: 2025-01-31*

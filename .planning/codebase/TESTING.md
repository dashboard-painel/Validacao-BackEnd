# Testing Patterns

**Analysis Date:** 2025-01-31

## Test Framework

**Runner:** None — no testing framework is installed or configured.

**Testing libraries in `requirements.txt`:** None detected.
```
fastapi>=0.109.0
uvicorn[standard]>=0.27.0
redshift-connector>=2.0.0
python-dotenv>=1.0.0
pydantic>=2.0.0
psycopg2-binary>=2.9.0
requests>=2.31.0
```
No `pytest`, `unittest`, `httpx`, `coverage`, `pytest-asyncio`, or any other testing library is listed.

**Test files found:** None. There is no `tests/` directory and no `test_*.py` or `*_test.py` files anywhere in the project source tree.

---

## Current State

**The project has zero automated tests.**

There are no:
- Unit tests
- Integration tests
- End-to-end tests
- Fixtures or factories
- Test configuration files (`pytest.ini`, `setup.cfg [tool:pytest]`, `pyproject.toml`)
- `conftest.py` files
- Mocking infrastructure

---

## Testability Assessment

Despite having no tests, the codebase has a layered architecture that supports incremental test addition. The following analysis identifies which layers are easiest to test and which require mocking.

### Highly Testable (Pure Logic — No External Dependencies)

#### `app/utils.py` — `camadas_atrasadas()`
The most immediately testable function in the codebase. Pure logic, no I/O, no side effects.

```python
# app/utils.py
def camadas_atrasadas(
    data_gold: Optional[str],
    data_silver: Optional[str],
    coletor_novo: Optional[str],
) -> Tuple[Optional[list[str]], Optional[list[str]]]:
```

**What to test:**
- Returns `(None, None)` when both dates are today (up-to-date)
- Returns `(["GoldVendas"], None)` when `data_gold` is before D-1
- Returns `(None, ["GoldVendas"])` when `data_gold` is `None`
- Returns `(["API"], None)` when `coletor_novo` starts with `"Pendente de envio no dia "` with a past date
- Returns correct combined lists when multiple camadas are affected
- Handles invalid date strings gracefully (no exception on `ValueError`)

#### `app/models/comparacao.py` — `Divergencia`, `ResultadoComparacao`
Plain dataclasses — can be instantiated and asserted on directly with no mocking.

#### `app/schemas.py` — All Pydantic Models
Pydantic v2 models can be tested for validation rules, optional fields, and `ge=0` constraints:
- `ComparacaoResponse`, `DivergenciaResponse`, `FarmaciaStatusResponse`
- `AssociacaoResumoResponse`, `ResultadoConsolidadoResponse`

```python
# Example test (not currently written)
def test_comparacao_response_rejects_negative_totals():
    with pytest.raises(ValidationError):
        ComparacaoResponse(associacao="80", total_gold_vendas=-1, ...)
```

---

### Testable with Mocking (Business Logic)

#### `app/services/comparacao_service.py` — `_comparar_resultados()`, `executar_comparacao()`
The core comparison logic (`_comparar_resultados`) builds divergence lists from two lists of dicts. The comparison algorithm can be tested by mocking `execute_gold_vendas` and `execute_silver_stgn_dedup` to return controlled data.

**What to test:**
- Pharmacy present in gold only → `tipo_divergencia == "apenas_gold_vendas"`
- Pharmacy present in silver only → `tipo_divergencia == "apenas_silver_stgn_dedup"`
- Same pharmacy in both with different dates → `tipo_divergencia == "data_diferente"`
- Same pharmacy in both with identical dates → NOT in divergencias
- Empty inputs → zero divergences, correct totals

```python
# Example mock target
from unittest.mock import patch

with patch("app.services.comparacao_service.execute_gold_vendas") as mock_gold:
    mock_gold.return_value = [{"cod_farmacia": "001", "ultima_venda": "2024-01-10", ...}]
    ...
```

#### `app/mappers/comparacao_mapper.py`
Mapper functions receive domain objects and return Pydantic schema instances. Fully testable by constructing `Divergencia` dataclass inputs and asserting on the returned `DivergenciaResponse` fields.

**Functions to test:**
- `montar_divergencia_response()` — `app/mappers/comparacao_mapper.py`
- `montar_status_farmacia_response()` — `app/mappers/comparacao_mapper.py`
- `montar_comparacao_response()` — `app/mappers/comparacao_mapper.py`
- `montar_resultado_consolidado()` — `app/mappers/comparacao_mapper.py` (note: case normalization from DB lowercase keys like `"ultima_venda_goldvendas"`)

---

### Testable with Test Database or Mocking (Repository/DB Layer)

#### `app/local_db.py`
Functions interact with a real PostgreSQL database. Can be tested with:
- A test PostgreSQL database (Docker-based)
- `unittest.mock.patch("app.local_db.get_local_connection")` to return an in-memory mock

**Functions to test:**
- `salvar_comparacao()` — verifies UPSERT behavior, ID return, orphan deletion
- `buscar_todos_consolidados()` — verifies join query returns correct shape
- `buscar_historico_por_associacao()` — verifies filter by `codigo_rede`
- `salvar_status_farmacias()` — verifies UPDATE of `coletor_novo`, `coletor_bi_ultima_data`, `coletor_bi_ultima_hora`
- `_sanitize_cnpj()` — pure function, no mocking needed

#### `app/database.py`
- `get_connection_config()` — can be unit-tested by setting/unsetting environment variables
- `test_connection()` — integration test requiring a real Redshift or mock connection

---

### Testable via FastAPI TestClient (Router / HTTP Layer)

#### `app/routers/comparar.py`
FastAPI provides a `TestClient` (via `httpx`) for router-level tests without spinning up a real server.

**Endpoints to test:**
- `GET /comparar?associacao=80` — happy path, error path (503 on exception)
- `POST /comparar` — body validation, service call
- `GET /historico` — empty list vs populated list
- `GET /historico/{associacao}` — 404 on missing associacao, list on found
- `GET /ultima-atualizacao` — returns `{"atualizado_em": ...}`
- `GET /coletor/{codigo}` — returns `{"data_hora_ultima_venda": ...}`
- `GET /` — root endpoint (no mocking needed)
- `GET /health` — mocks `test_connection()` to return `{"connected": True}`

```python
# Example (not currently written)
from fastapi.testclient import TestClient
from unittest.mock import patch
from app.main import app

client = TestClient(app)

def test_comparar_returns_503_on_db_error():
    with patch("app.routers.comparar.executar_comparacao", side_effect=Exception("db down")):
        response = client.get("/comparar?associacao=80")
    assert response.status_code == 503
```

---

### Requires Network Mocking (External Client Layer)

#### `app/clients/business_connect.py`
Uses `requests` library to call `https://business-connect.triercloud.com.br/v1`. Use `responses` library or `unittest.mock.patch("requests.get")` to test without real HTTP.

**Functions to test:**
- `get_bearer_token()` — tests auth failure, missing token keys, successful token extraction
- `get_status_farmacia()` — tests 404 → "OK, sem registro", 200 with `cadcvend` record, 200 without `cadcvend`, HTTP error
- `buscar_status_farmacias()` — tests empty input early return, parallel execution with mixed results

#### `app/clients/coletor_bi.py`
Uses `requests` library for Coletor BI API.

**Functions to test:**
- `buscar_por_codigo()` — tests timeout exception → fallback dict, non-200 response, empty `dat_emissao` items, `max()` selection of most recent record
- `buscar_por_associacao()` — tests empty input, parallel dispatch

---

## Recommended Test Setup

### Install Testing Dependencies
Add to `requirements.txt` (or a separate `requirements-dev.txt`):
```
pytest>=8.0.0
pytest-cov>=5.0.0
httpx>=0.27.0          # Required by FastAPI TestClient
responses>=0.25.0      # For mocking requests library
```

### Recommended Directory Structure
```
Validacao-BackEnd/
├── app/
├── tests/
│   ├── __init__.py
│   ├── conftest.py                    # Shared fixtures
│   ├── test_utils.py                  # app/utils.py — camadas_atrasadas()
│   ├── test_schemas.py                # app/schemas.py — Pydantic validation
│   ├── test_mappers.py                # app/mappers/comparacao_mapper.py
│   ├── test_comparacao_service.py     # app/services/comparacao_service.py
│   ├── test_business_connect.py       # app/clients/business_connect.py
│   ├── test_coletor_bi.py             # app/clients/coletor_bi.py
│   ├── test_database.py               # app/database.py
│   └── test_routers.py                # app/routers/comparar.py (TestClient)
└── requirements.txt
```

### Run Commands (once pytest is installed)
```bash
pytest                          # Run all tests
pytest tests/test_utils.py      # Run a single file
pytest --cov=app                # Run with coverage report
pytest --cov=app --cov-report=html  # HTML coverage output
```

---

## Priority Order for Test Coverage

| Priority | Area | Rationale |
|---|---|---|
| 🔴 High | `app/utils.py` — `camadas_atrasadas()` | Pure logic, high business value, zero setup cost |
| 🔴 High | `app/services/comparacao_service.py` — `_comparar_resultados()` | Core comparison algorithm; divergence classification bugs would be silent |
| 🔴 High | `app/mappers/comparacao_mapper.py` — `montar_resultado_consolidado()` | Maps lowercased DB column names (e.g., `ultima_venda_goldvendas`) back to mixed-case schema fields — subtle mapping bugs |
| 🟡 Medium | `app/routers/comparar.py` | HTTP contract tests; verifies 503/404 behavior |
| 🟡 Medium | `app/clients/business_connect.py` — `get_status_farmacia()` | Fallback logic must handle all HTTP variants correctly |
| 🟡 Medium | `app/clients/coletor_bi.py` — `buscar_por_codigo()` | Date parsing with `datetime.strptime` can raise on unexpected formats |
| 🟢 Low | `app/database.py` — `get_connection_config()` | Env var validation; simple to test but low risk |
| 🟢 Low | `app/schemas.py` | Pydantic handles most validation automatically |
| 🟢 Low | `app/local_db.py` | Requires DB; integration test; lower ROI for unit testing |

---

## Notable Testing Risks

### Silent Divergence Miscounts
`_comparar_resultados()` in `app/services/comparacao_service.py` uses string comparison (`venda_gold != venda_silver`) for dates. If either value has unexpected whitespace or different date formats (e.g., `"2024-01-10"` vs `"2024-01-10 00:00:00"`), divergences will be falsely counted or missed. This logic has no tests.

### Column Name Case Normalization in Mapper
`montar_resultado_consolidado()` in `app/mappers/comparacao_mapper.py` accesses dict keys in lowercase (`row.get("ultima_venda_goldvendas")`), while the Pydantic schema field names use mixed case (`ultima_venda_GoldVendas`). If the DB column name ever changes, the mapper silently returns `None` instead of raising an error. No tests exist to catch this.

### Date Parsing in `coletor_bi.py`
`buscar_por_codigo()` in `app/clients/coletor_bi.py` uses `datetime.strptime(f"{x['dat_emissao']} {x['hor_emissao']}", "%Y-%m-%d %H:%M:%S")` inside a `max()` key function with no exception handling. If the API returns a record with a malformed date, the entire function call raises and the exception propagates up.

---

*Testing analysis: 2025-01-31*

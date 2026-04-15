# Validacao-BackEnd

API em Python (FastAPI) para comparação de dados entre queries no Amazon Redshift.

## Requisitos

- Python 3.11+
- Acesso ao cluster Amazon Redshift
- PostgreSQL local (para histórico de comparações)

## Setup

1. Clone o repositório
2. Crie um ambiente virtual:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   venv\Scripts\activate     # Windows
   ```
3. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```
4. Configure as variáveis de ambiente:
   ```bash
   cp .env.example .env
   # Edite .env com suas credenciais do Redshift e PostgreSQL local
   ```

## Executando

```bash
uvicorn app.main:app --reload
```

A API estará disponível em `http://localhost:8000`

## Endpoints

### `GET /` — Informações da API

Retorna informações básicas sobre a API.

### `GET /health` — Health Check

Verifica o status da API e conexão com o Redshift.

### `GET /comparar` — Comparar Dados

Compara os resultados das duas queries no Redshift e retorna as divergências encontradas.

**Parâmetros de Query:**

| Parâmetro | Tipo | Obrigatório | Descrição |
|-----------|------|-------------|-----------|
| `associacao` | string | Sim | Código da associação para filtrar |
| `dat_emissao` | string | Não | Data mínima (YYYY-MM-DD). Default: 30 dias atrás |

**Exemplo:**

```bash
curl "http://localhost:8000/comparar?associacao=123&dat_emissao=2024-01-01"

# Apenas com associacao (dat_emissao usa default de 30 dias)
curl "http://localhost:8000/comparar?associacao=123"
```

**Resposta (200):**
```json
{
  "associacao": "123",
  "dat_emissao_filtro": "2024-01-01",
  "total_q1": 150,
  "total_q2": 148,
  "total_divergencias": 5,
  "comparacao_id": 42,
  "divergencias": [
    {
      "cod_farmacia": "001",
      "nome_farmacia": "Farmacia Central",
      "ultima_venda_q1": "2024-03-15",
      "ultima_hora_venda_q1": "14:30:00",
      "ultima_venda_q2": "2024-03-14",
      "ultima_hora_venda_q2": "09:00:00",
      "tipo_divergencia": "data_diferente"
    }
  ]
}
```

**Tipos de Divergência:**
- `data_diferente` — Farmácia presente em ambas as queries mas com datas diferentes
- `apenas_q1` — Farmácia presente somente na Query 1 (associacao.vendas)
- `apenas_q2` — Farmácia presente somente na Query 2 (silver.cadcvend_staging_dedup)

**Erros:**

| Código | Descrição |
|--------|-----------|
| 422 | `associacao` ausente ou `dat_emissao` em formato inválido |
| 503 | Erro de conexão com o Redshift |

## Documentação Interativa

Com a API rodando, acesse:
- **Swagger UI:** http://localhost:8000/docs
- **ReDoc:** http://localhost:8000/redoc

## Variáveis de Ambiente

| Variável | Descrição | Obrigatória | Default |
|----------|-----------|-------------|---------|
| REDSHIFT_HOST | Endpoint do cluster Redshift | Sim | — |
| REDSHIFT_PORT | Porta do Redshift | Sim | — |
| REDSHIFT_DATABASE | Nome do banco de dados | Sim | — |
| REDSHIFT_USER | Usuário do Redshift | Sim | — |
| REDSHIFT_PASSWORD | Senha do Redshift | Sim | — |
| REDSHIFT_SCHEMA | Schema do Redshift | Não | public |
| LOCAL_DB_URL | URL do PostgreSQL local | Sim | — |
| CORS_ORIGINS | Origins permitidos para CORS | Não | `*` |

`CORS_ORIGINS` aceita `*` (todas as origens) ou lista separada por vírgulas: `http://localhost:3000,https://app.example.com`

## Estrutura do Projeto

```
app/
├── __init__.py
├── main.py           # Entry point FastAPI
├── database.py       # Conexão com Redshift
├── queries.py        # Queries SQL (Q1 e Q2)
├── comparador.py     # Lógica de comparação
├── local_db.py       # Persistência no PostgreSQL local
├── schemas.py        # Modelos Pydantic
└── routers/
    ├── __init__.py
    └── comparar.py   # Endpoint /comparar
```

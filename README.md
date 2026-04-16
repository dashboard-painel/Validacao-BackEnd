# Validacao-BackEnd

API em Python (FastAPI) para comparação da última venda registrada por farmácia entre duas fontes do Amazon Redshift: `associacao.vendas` (GoldVendas) e `silver.cadcvend_staging_dedup` (SilverSTGN_Dedup).

## Requisitos

- Python 3.12+
- Acesso ao cluster Amazon Redshift
- PostgreSQL local (para histórico de comparações)
- Acesso à API Business Connect (para status de migração e dados cadastrais)

## Setup

1. Clone o repositório
2. Crie um ambiente virtual:
   ```bash
   python -m venv .venv
   .venv\Scripts\activate     # Windows
   source .venv/bin/activate  # Linux/Mac
   ```
3. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```
4. Configure as variáveis de ambiente:
   ```bash
   cp .env.example .env
   # Edite .env com suas credenciais
   ```

## Executando

```bash
uvicorn app.main:app --reload
```

A API estará disponível em `http://localhost:8000`

## Endpoints

### `GET /` — Informações da API

Retorna nome, versão e status da API.

### `GET /health` — Health Check

Verifica o status da API e da conexão com o Redshift.

### `GET /comparar` — Comparar Dados

Busca a **última venda registrada** por farmácia em cada fonte e retorna as divergências.

**Parâmetros de Query:**

| Parâmetro | Tipo | Obrigatório | Descrição |
|-----------|------|-------------|-----------|
| `associacao` | string | Sim | Código da associação para filtrar |

**Exemplo:**
```bash
curl "http://localhost:8000/comparar?associacao=123"
```

**Resposta (200):**
```json
{
  "associacao": "123",
  "total_gold_vendas": 150,
  "total_silver_stgn_dedup": 148,
  "total_divergencias": 5,
  "comparacao_id": 42,
  "divergencias": [
    {
      "cod_farmacia": "001",
      "nome_farmacia": "Farmacia Central",
      "cnpj": "12345678000199",
      "ultima_venda_GoldVendas": "2024-03-15",
      "ultima_hora_venda_GoldVendas": "14:30:00",
      "ultima_venda_SilverSTGN_Dedup": "2024-03-14",
      "ultima_hora_venda_SilverSTGN_Dedup": "09:00:00",
      "tipo_divergencia": "data_diferente"
    }
  ],
  "status_farmacias": [
    {
      "cod_farmacia": "001",
      "coletor_novo": "Pendente de envio no dia 2024-03-14"
    }
  ]
}
```

**Tipos de Divergência:**
- `data_diferente` — Farmácia presente em ambas as fontes mas com datas distintas
- `apenas_gold_vendas` — Farmácia presente somente em `associacao.vendas`
- `apenas_silver_stgn_dedup` — Farmácia presente somente em `silver.cadcvend_staging_dedup`

**Erros:**

| Código | Descrição |
|--------|-----------|
| 422 | `associacao` ausente |
| 503 | Erro de conexão com o Redshift |

### `POST /comparar` — Comparar via JSON

Mesma lógica do GET, mas recebe parâmetros no body:

```bash
curl -X POST "http://localhost:8000/comparar" \
  -H "Content-Type: application/json" \
  -d '{"associacao": "123"}'
```

### `GET /historico` — Tabela completa

Retorna todas as farmácias de todas as associações (última comparação de cada uma), com `coletor_novo` e `tipo_divergencia` embutidos. Ideal para carregar o dashboard sem parâmetros.

```bash
curl "http://localhost:8000/historico"
```

### `GET /historico/{associacao}` — Filtro por associação

Mesma estrutura do `GET /historico`, mas filtrado para uma associação específica.

```bash
curl "http://localhost:8000/historico/123"
```

**Campos da resposta (ambos os endpoints):**

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `associacao` | string | Código da associação |
| `cod_farmacia` | string | Código da farmácia |
| `nome_farmacia` | string \| null | Nome da farmácia |
| `cnpj` | string \| null | CNPJ sem formatação (14 dígitos) |
| `ultima_venda_GoldVendas` | string \| null | Última venda em `associacao.vendas` |
| `ultima_hora_venda_GoldVendas` | string \| null | Hora da última venda em `associacao.vendas` |
| `ultima_venda_SilverSTGN_Dedup` | string \| null | Última venda em `silver.cadcvend_staging_dedup` |
| `ultima_hora_venda_SilverSTGN_Dedup` | string \| null | Hora da última venda em `silver.cadcvend_staging_dedup` |
| `coletor_novo` | string \| null | Status no Business Connect |
| `tipo_divergencia` | string \| null | Tipo de divergência (`null` = sem divergência) |

**Erros:**

| Código | Descrição |
|--------|-----------|
| 404 | Associação não encontrada (apenas `/historico/{associacao}`) |
| 503 | Erro ao acessar o banco local |

## Fluxo Interno

```
POST /comparar?associacao=X  (ou GET)
       │
       ├─ Redshift [GoldVendas]        → última venda por farmácia em associacao.vendas
       ├─ Redshift [SilverSTGN_Dedup]  → última venda por farmácia em silver.cadcvend_staging_dedup
       ├─ Redshift [cadfilia/dimensao] → enriquece nome/CNPJ de farmácias silver-only
       ├─ Comparação por cod_farmacia  → detecta divergências
       ├─ Business Connect (paralelo)  → status de migração de todas as farmácias
       └─ PostgreSQL local             → persiste resultado e histórico

GET /historico
       └─ PostgreSQL local             → última comparação de cada associação, com JOIN
                                         em status_farmacias e divergencias
```

## Observações

- O CNPJ é salvo **sem formatação** (somente os 14 dígitos) na tabela `resultados_consolidados`.
- Dados cadastrais (nome/CNPJ) de farmácias silver-only são enriquecidos via `silver.cadfilia_staging_dedup` com fallback para `associacao.dimensao_cadastro_lojas`.
- Todos os passos críticos emitem logs com tempo de resposta no formato `⏳ Aguardando... / ✅ respondeu em Xs`.

## Documentação Interativa

Com a API rodando, acesse:
- **Swagger UI:** http://localhost:8000/docs
- **ReDoc:** http://localhost:8000/redoc

## Variáveis de Ambiente

| Variável | Descrição | Obrigatória | Default |
|----------|-----------|-------------|---------|
| `REDSHIFT_HOST` | Endpoint do cluster Redshift | Sim | — |
| `REDSHIFT_PORT` | Porta do Redshift | Não | `5439` |
| `REDSHIFT_DATABASE` | Nome do banco (ou `REDSHIFT_NAME`) | Sim | — |
| `REDSHIFT_USER` | Usuário do Redshift | Sim | — |
| `REDSHIFT_PASSWORD` | Senha do Redshift (ou `REDSHIFT_PASS`) | Sim | — |
| `LOCAL_DB_URL` | URL do PostgreSQL local | Sim | — |
| `LOCAL_DB_USER` | Usuário do PostgreSQL local | Não | — |
| `LOCAL_DB_PASS` | Senha do PostgreSQL local | Não | — |
| `BC_USERNAME` | Usuário da API Business Connect | Sim | — |
| `BC_PASSWORD` | Senha da API Business Connect | Sim | — |
| `CORS_ORIGINS` | Origins permitidos para CORS | Não | `*` |

`CORS_ORIGINS` aceita `*` (todas as origens) ou lista separada por vírgulas: `http://localhost:3000,https://app.example.com`

## Estrutura do Projeto

```
app/
├── __init__.py
├── main.py              # Entry point FastAPI + configuração de logging
├── database.py          # Conexão com Redshift
├── queries.py           # Queries SQL e enriquecimento cadastral
├── comparador.py        # Lógica de comparação entre as duas fontes
├── business_connect.py  # Integração com a API Business Connect
├── local_db.py          # Persistência no PostgreSQL local
├── schemas.py           # Modelos Pydantic
└── routers/
    ├── __init__.py
    └── comparar.py      # Endpoints /comparar e /historico
```

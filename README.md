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
5. Na primeira execução (ou após alterar o schema), o banco local é criado automaticamente ao iniciar a API. Se precisar recriar do zero, rode no psql:
   ```sql
   DROP TABLE IF EXISTS status_farmacias, divergencias, resultados_consolidados,
     resultados_silver_stgn_dedup, resultados_gold_vendas, comparacoes CASCADE;
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
  "total_divergencias": 5,
  "comparacao_id": 42,
  "divergencias": [
    {
      "cod_farmacia": "001",
      "nome_farmacia": "Farmacia Central",
      "cnpj": "12345678000199",
      "ultima_venda_GoldVendas": "2024-03-15",
      "ultima_hora_venda_GoldVendas": "14:30:00",
      "tipo_divergencia": "apenas_gold_vendas",
      "camadas_atrasadas": ["GoldVendas"],
      "camadas_sem_dados": null
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
- `apenas_gold_vendas` — Farmácia presente somente em `associacao.vendas`

**Regra de atraso (`camadas_atrasadas`):**  
Uma camada é considerada atrasada se a última venda for **anterior a D-1** (D-2 ou mais antigo) — mesmo critério para GoldVendas, SilverSTGN_Dedup e API.

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

Retorna todas as farmácias de todas as associações (última comparação de cada uma), com `coletor_novo`, `tipo_divergencia`, `camadas_atrasadas`, `camadas_sem_dados` e `atualizado_em` embutidos. Ideal para carregar o dashboard sem parâmetros.

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
| `nome_farmacia` | string \| null | Nome fantasia da farmácia |
| `cnpj` | string \| null | CNPJ sem formatação (14 dígitos) |
| `ultima_venda_GoldVendas` | string \| null | Última venda em `associacao.vendas` |
| `ultima_hora_venda_GoldVendas` | string \| null | Hora da última venda em `associacao.vendas` |
| `ultima_venda_SilverSTGN_Dedup` | string \| null | Última venda em `silver.cadcvend_staging_dedup` |
| `ultima_hora_venda_SilverSTGN_Dedup` | string \| null | Hora da última venda em `silver.cadcvend_staging_dedup` |
| `camadas_atrasadas` | string[] \| null | Camadas com dado desatualizado (D-2 ou mais antigo) |
| `camadas_sem_dados` | string[] \| null | Camadas sem nenhum registro de venda |
| `tipo_divergencia` | string \| null | Tipo de divergência (`null` = sem divergência) |
| `num_versao` | string \| null | Versão do coletor via Sicfarma `/versoes` (`null` se não encontrado) |
| `classificacao` | string \| null | Classificação Sicfarma da farmácia (ex: `GOLD`, `PRIME`, `SELECT1`) — `null` se não cadastrada |
| `coletor_novo` | string \| null | Status no Business Connect |
| `atualizado_em` | string \| null | Data/hora em que a comparação foi executada |

**Erros:**

| Código | Descrição |
|--------|-----------|
| 404 | Associação não encontrada (apenas `/historico/{associacao}`) |
| 503 | Erro ao acessar o banco local |

### `GET /ultima-atualizacao` — Última atualização do sistema

Retorna a data/hora da comparação mais recente entre **todas** as associações. Útil para exibir um badge de "última atualização" no dashboard.

```bash
curl "http://localhost:8000/ultima-atualizacao"
```

**Resposta (200):**
```json
{ "atualizado_em": "2026-04-17T10:30:00" }
```

Retorna `null` em `atualizado_em` se nenhuma comparação foi realizada ainda.

## Fluxo Interno

```
POST /comparar?associacao=X  (ou GET)
       │
       ├─ Redshift [GoldVendas]              → última venda por farmácia em associacao.vendas
       ├─ Comparação por cod_farmacia        → detecta divergências
       ├─ Business Connect (paralelo)        → status de migração de todas as farmácias
       ├─ Sicfarma /classificacao (paralelo) → classificação da farmácia (GOLD, PRIME, etc.)
       ├─ Sicfarma /versoes (paralelo)       → versão do coletor instalado (num_versao)
       └─ PostgreSQL local                   → UPSERT por (associacao, cod_farmacia)

GET /historico
       └─ PostgreSQL local                   → última comparação de cada associação
```

## Observações

- O CNPJ é salvo **sem formatação** (somente os 14 dígitos) na tabela `resultados_consolidados`.
- O nome da farmácia usa `nom_fantasia` (nome fantasia).
- IDs de farmácias no banco local são **estáveis entre rodadas** — o UPSERT em `(associacao, cod_farmacia)` garante que o mesmo registro é reutilizado.
- A tabela `comparacoes` é **append-only**: cada rodada gera uma nova linha de histórico.
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
| `SICFARMA_URL` | URL base da API Sicfarma (ex: `.../api/farmacias`) | Sim | — |
| `SICFARMA_USERNAME` | Usuário da API Sicfarma | Sim | — |
| `SICFARMA_PASSWORD` | Senha da API Sicfarma | Sim | — |
| `CORS_ORIGINS` | Origins permitidos para CORS | Não | `*` |

`CORS_ORIGINS` aceita `*` (todas as origens) ou lista separada por vírgulas: `http://localhost:3000,https://app.example.com`

## Estrutura do Projeto

```
app/
├── main.py                        # Entry point FastAPI + logging
├── utils.py                       # Helpers (ex: cálculo de camadas atrasadas)
├── db/
│   ├── redshift.py                # Conexão com o Amazon Redshift
│   └── local.py                   # Conexão e operações no PostgreSQL local
├── api/
│   └── comparacao.py              # Endpoints: /comparar, /historico, /vendas-parceiros, etc.
├── core/
│   ├── comparacao.py              # Lógica de comparação + conversão para response
│   └── vendas.py                  # Lógica de vendas parceiros
├── models/
│   ├── comparacao.py              # Modelos internos (dataclasses) + schemas Pydantic
│   └── vendas.py                  # Schemas Pydantic de vendas parceiros
├── clients/
│   ├── business_connect.py        # Integração com a API Business Connect
│   ├── sicfarma.py                # Integração com a API Sicfarma
│   └── coletor_bi.py              # Integração com o Coletor BI (desativado)
└── repositories/
    ├── redshift_repository.py     # Queries no Redshift
    └── comparacao_repository.py   # Queries no banco local
```

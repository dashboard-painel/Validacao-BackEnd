# Guia de Integração — Frontend

API rodando em `http://localhost:8000` (ajustar para o host de produção).

---

## Fluxo recomendado para o dashboard

```
1. Ao abrir o frontend
   ├─ GET /ultima-atualizacao  →  badge "Última atualização: ..."
   └─ GET /historico           →  tabela com TODAS as farmácias de TODAS as associações

2. Usuário quer filtrar por uma associação específica
   └─ GET /historico/{associacao}  →  mesma estrutura, só uma associação

3. Usuário dispara nova comparação (botão "Atualizar")
   └─ POST /comparar  →  executa queries no Redshift e atualiza o banco local
      └─ após o POST, recarregue GET /historico e GET /ultima-atualizacao
```

---

## Endpoints

### `GET /ultima-atualizacao` — Badge de última atualização

Retorna a data/hora da comparação mais recente entre **todas** as associações.

**Resposta (200):**
```json
{ "atualizado_em": "2026-04-17T10:30:00" }
```

`atualizado_em` é `null` se nenhuma comparação foi realizada ainda.

---

### `GET /historico` — Todas as farmácias de todas as associações

**Resposta (200):** array de objetos `ResultadoConsolidado`

```json
[
  {
    "associacao": "80",
    "cod_farmacia": "30559",
    "nome_farmacia": "FRANQUIA PLANALTO",
    "cnpj": "12345678000199",
    "sit_contrato": "ATIVO",
    "codigo_rede": "80",
    "ultima_venda_GoldVendas": "2026-04-08",
    "ultima_hora_venda_GoldVendas": "2026-04-08 18:30:00",
    "ultima_venda_SilverSTGN_Dedup": "2026-04-14",
    "ultima_hora_venda_SilverSTGN_Dedup": "18:55:10",
    "coletor_novo": "Pendente de envio no dia 2026-04-10",
    "coletor_bi_ultima_data": "2026-04-10",
    "coletor_bi_ultima_hora": "08:00:00",
    "tipo_divergencia": "data_diferente",
    "camadas_atrasadas": ["GoldVendas", "API"],
    "camadas_sem_dados": null,
    "atualizado_em": "2026-04-17T10:30:00"
  }
]
```

---

### `GET /historico/{associacao}` — Farmácias de uma associação

**URL:** `GET /historico/80`

Mesma estrutura acima, filtrada por uma associação. Retorna `404` se não houver comparação salva para essa associação.

---

### `POST /comparar` — Disparar nova comparação

**Body:**
```json
{ "associacao": "80" }
```

**Resposta (200):**
```json
{
  "associacao": "80",
  "total_gold_vendas": 64,
  "total_silver_stgn_dedup": 74,
  "total_divergencias": 36,
  "comparacao_id": 3,
  "divergencias": [
    {
      "cod_farmacia": "30559",
      "nome_farmacia": "FRANQUIA PLANALTO",
      "cnpj": "12345678000199",
      "sit_contrato": "ATIVO",
      "codigo_rede": "80",
      "ultima_venda_GoldVendas": "2026-04-08",
      "ultima_hora_venda_GoldVendas": "2026-04-08 18:30:00",
      "ultima_venda_SilverSTGN_Dedup": "2026-04-14",
      "ultima_hora_venda_SilverSTGN_Dedup": "18:55:10",
      "tipo_divergencia": "data_diferente",
      "camadas_atrasadas": ["GoldVendas"],
      "camadas_sem_dados": null
    }
  ],
  "status_farmacias": [
    {
      "cod_farmacia": "30559",
      "coletor_novo": "Pendente de envio no dia 2026-04-10",
      "coletor_bi_ultima_data": "2026-04-10",
      "coletor_bi_ultima_hora": "08:00:00"
    }
  ]
}
```

> Após o POST, chame `GET /historico` (ou `/{associacao}`) e `GET /ultima-atualizacao` para atualizar a tela.

**Erros:**

| Código | Descrição |
|--------|-----------|
| `422` | `associacao` não informado |
| `503` | Falha de conexão com o Redshift |

---

### `GET /comparar?associacao=80` — Alternativa via query param

Mesmo resultado do POST, sem body.

---

### Outros endpoints

| Endpoint | Uso |
|----------|-----|
| `GET /` | Verifica se a API está no ar |
| `GET /health` | Status + conectividade com Redshift |
| `GET /docs` | Swagger UI interativo |

---

## Campos da resposta

### `/historico` e `POST /comparar → divergencias`

| Campo | Tipo | Origem | Descrição |
|-------|------|--------|-----------|
| `associacao` | string | `comparacoes` | Código da associação |
| `cod_farmacia` | string | Redshift | Código da farmácia |
| `nome_farmacia` | string \| null | `dimensao_cadastro_lojas` ¹ | Nome da farmácia |
| `cnpj` | string \| null | `dimensao_cadastro_lojas` ¹ | CNPJ somente dígitos (sem `.` `-` `/`) |
| `sit_contrato` | string \| null | `dimensao_cadastro_lojas` | Situação do contrato (ex: `"ATIVO"`, `"INATIVO"`) |
| `codigo_rede` | string \| null | `dimensao_cadastro_lojas` | Código da rede — igual ao código de associação |
| `ultima_venda_GoldVendas` | string \| null | `associacao.vendas` | Data da última venda (formato `YYYY-MM-DD`) |
| `ultima_hora_venda_GoldVendas` | string \| null | `associacao.vendas` | Hora da última venda |
| `ultima_venda_SilverSTGN_Dedup` | string \| null | `silver.cadcvend_staging_dedup` | Data da última venda (formato `YYYY-MM-DD`) |
| `ultima_hora_venda_SilverSTGN_Dedup` | string \| null | `silver.cadcvend_staging_dedup` | Hora da última venda |
| `coletor_novo` | string \| null | Business Connect | Status do coletor (ver tabela abaixo) |
| `coletor_bi_ultima_data` | string \| null | Coletor BI | Data da última venda no Coletor BI |
| `coletor_bi_ultima_hora` | string \| null | Coletor BI | Hora da última venda no Coletor BI |
| `tipo_divergencia` | string \| null | — | Tipo de divergência (ver tabela abaixo) |
| `camadas_atrasadas` | string[] \| null | — | Camadas com dado desatualizado (D-2 ou mais antigo) |
| `camadas_sem_dados` | string[] \| null | — | Camadas sem nenhum registro de venda |
| `atualizado_em` | string \| null | — | Data/hora em que a comparação foi executada |

> ¹ `nome_farmacia` e `cnpj` vêm de `dimensao_cadastro_lojas` com fallback para `silver.cadfilia_staging_dedup` quando nulos.

---

## Detalhamento dos campos de status

### `sit_contrato` — Novidade

Situação contratual da farmácia conforme cadastro na `dimensao_cadastro_lojas`. Pode ser `null` se a farmácia não estiver na dimensão.

Use para filtrar/destacar farmácias inativas no dashboard.

### `codigo_rede` — Novidade

Código da rede ao qual a farmácia pertence. Equivale ao código de associação usado na consulta. Pode ser `null` para farmácias presentes somente na Silver sem cadastro na dimensão.

### `tipo_divergencia`

| Valor | Significa |
|-------|-----------|
| `null` | Sem divergência — datas iguais nas duas fontes |
| `"data_diferente"` | Presente nas duas fontes, mas com datas distintas |
| `"apenas_gold_vendas"` | Presente somente em `associacao.vendas` |
| `"apenas_silver_stgn_dedup"` | Presente somente em `silver.cadcvend_staging_dedup` |

### `camadas_atrasadas`

Indica quais camadas têm dado **presente mas desatualizado** (D-2 ou mais antigo).

| Valor | Camada | Critério |
|-------|--------|----------|
| `"GoldVendas"` | `associacao.vendas` | `ultima_venda_GoldVendas` < D-1 |
| `"SilverSTGN_Dedup"` | `silver.cadcvend_staging_dedup` | `ultima_venda_SilverSTGN_Dedup` < D-1 |
| `"API"` | Business Connect | `"Pendente de envio"` com data D-2 ou mais antiga |

### `camadas_sem_dados`

| Valor | Significa |
|-------|-----------|
| `"GoldVendas"` | `ultima_venda_GoldVendas` é `null` |
| `"SilverSTGN_Dedup"` | `ultima_venda_SilverSTGN_Dedup` é `null` |

### `coletor_novo`

| Valor | Significa |
|-------|-----------|
| `"OK, sem registro"` | Farmácia sem pendência de envio |
| `"Pendente de envio no dia YYYY-MM-DD"` | Pendência registrada |
| `"Indisponível"` | Business Connect não respondeu (temporário) |

---

## CORS

A API já tem CORS habilitado (`Access-Control-Allow-Origin: *`).
Chamadas diretas do browser funcionam sem configuração adicional.

Para restringir a origens específicas, ajuste `CORS_ORIGINS` no `.env`:
```
CORS_ORIGINS=http://localhost:3000,https://dashboard.suaempresa.com
```

---

## O que mudou nesta versão

| Campo | Antes | Agora |
|-------|-------|-------|
| `nome_farmacia` | vinha direto de `associacao.vendas` | vem de `dimensao_cadastro_lojas` (fallback: `cadfilia_staging_dedup`) |
| `cnpj` | vinha direto de `associacao.vendas` | vem de `dimensao_cadastro_lojas` (fallback: `cadfilia_staging_dedup`) |
| `sit_contrato` | ❌ não existia | ✅ novo — de `dimensao_cadastro_lojas` |
| `codigo_rede` | ❌ não existia | ✅ novo — de `dimensao_cadastro_lojas` |

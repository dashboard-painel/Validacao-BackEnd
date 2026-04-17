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

**URL:**
```
GET http://localhost:8000/ultima-atualizacao
```

**Resposta (200):**
```json
{ "atualizado_em": "2026-04-17T10:30:00" }
```

`atualizado_em` é `null` se nenhuma comparação foi realizada ainda.

**Exemplo JS:**
```js
const res = await fetch('http://localhost:8000/ultima-atualizacao');
const { atualizado_em } = await res.json();
// Exibe no badge: `Última atualização: ${new Date(atualizado_em).toLocaleString()}`
```

---

### `GET /historico` — Todas as farmácias de todas as associações

Tela inicial: carrega automaticamente a tabela completa com todas as farmácias de todas as associações que possuem comparações salvas.

**URL:**
```
GET http://localhost:8000/historico
```

**Resposta (200):** array de farmácias

```json
[
  {
    "associacao": "80",
    "cod_farmacia": "30559",
    "nome_farmacia": "FRANQUIA PLANALTO",
    "cnpj": "12345678000199",
    "ultima_venda_GoldVendas": "2026-04-08",
    "ultima_hora_venda_GoldVendas": "2026-04-08 18:30:00",
    "ultima_venda_SilverSTGN_Dedup": "2026-04-14",
    "ultima_hora_venda_SilverSTGN_Dedup": "18:55:10",
    "coletor_novo": "Pendente de envio no dia 2026-04-10",
    "tipo_divergencia": "data_diferente",
    "camadas_atrasadas": ["GoldVendas", "API"],
    "camadas_sem_dados": null,
    "atualizado_em": "2026-04-17T10:30:00"
  },
  {
    "associacao": "120",
    "cod_farmacia": "11111",
    "nome_farmacia": "FARMÁCIA CENTRAL",
    "cnpj": "98765432000100",
    "ultima_venda_GoldVendas": "2026-04-14",
    "ultima_hora_venda_GoldVendas": "2026-04-14 09:10:00",
    "ultima_venda_SilverSTGN_Dedup": null,
    "ultima_hora_venda_SilverSTGN_Dedup": null,
    "coletor_novo": "OK, sem registro",
    "tipo_divergencia": "apenas_gold_vendas",
    "camadas_atrasadas": null,
    "camadas_sem_dados": ["SilverSTGN_Dedup"],
    "atualizado_em": "2026-04-17T10:30:00"
  }
]
```

**Exemplo JS:**
```js
const res = await fetch('http://localhost:8000/historico');
const farmacias = await res.json();
// Renderiza tabela com todas as farmácias
// Para filtrar por associação no frontend: farmacias.filter(f => f.associacao === '80')
```

---

### `GET /historico/{associacao}` — Farmácias de uma associação específica

Retorna a tabela da **última comparação** de uma associação.

**URL:**
```
GET http://localhost:8000/historico/80
```

**Resposta (200):** mesma estrutura de `/historico`, com campo `associacao` preenchido.

**Erros:**

| Código | Descrição |
|--------|-----------|
| `404` | Nenhuma comparação encontrada para a associação |
| `503` | Erro ao acessar o banco local |

**Exemplo JS:**
```js
async function buscarDetalhes(associacao) {
  const res = await fetch(`http://localhost:8000/historico/${associacao}`);
  if (!res.ok) throw new Error(`Erro ${res.status}`);
  return res.json(); // array de farmácias
}
```

---

## Campos da resposta `/historico`

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `associacao` | string | Código da associação |
| `cod_farmacia` | string | Código da farmácia |
| `nome_farmacia` | string \| null | Razão social da farmácia |
| `cnpj` | string \| null | CNPJ somente com dígitos (sem `.` `-` `/`) |
| `ultima_venda_GoldVendas` | string \| null | Última venda em `associacao.vendas` |
| `ultima_hora_venda_GoldVendas` | string \| null | Hora da última venda em `associacao.vendas` |
| `ultima_venda_SilverSTGN_Dedup` | string \| null | Última venda em `silver.cadcvend_staging_dedup` |
| `ultima_hora_venda_SilverSTGN_Dedup` | string \| null | Hora da última venda em `silver.cadcvend_staging_dedup` |
| `coletor_novo` | string \| null | Status no Business Connect (ver tabela abaixo) |
| `tipo_divergencia` | string \| null | Tipo de divergência — `null` se não há divergência |
| `camadas_atrasadas` | string[] \| null | Camadas com dado desatualizado (D-2 ou mais antigo) — `null` se nenhuma |
| `camadas_sem_dados` | string[] \| null | Camadas sem nenhum registro de venda — `null` se todas têm dados |
| `atualizado_em` | string \| null | Data/hora em que a comparação desta farmácia foi executada |

---

## Detalhamento dos campos de status

### `camadas_atrasadas`

Indica quais camadas têm dado **presente mas desatualizado** (D-2 ou mais antigo).  
Regra uniforme para todas as camadas: `ultima_venda < D-1`.

| Valor | Camada | Critério |
|-------|--------|----------|
| `"GoldVendas"` | `associacao.vendas` | `ultima_venda_GoldVendas` é D-2 ou mais antigo |
| `"SilverSTGN_Dedup"` | `silver.cadcvend_staging_dedup` | `ultima_venda_SilverSTGN_Dedup` é D-2 ou mais antigo |
| `"API"` | Business Connect | `"Pendente de envio"` com data D-2 ou mais antiga |

Exemplos:
- `["GoldVendas", "API"]` — atraso em Gold e no coletor
- `["SilverSTGN_Dedup"]` — atraso somente em Silver
- `null` — sem atraso em nenhuma camada

### `camadas_sem_dados`

Indica quais camadas **não têm nenhum registro** de venda (campo `null`).  
Mais crítico que `camadas_atrasadas` — a farmácia nunca enviou dados para essa camada.

| Valor | Significa |
|-------|-----------|
| `"GoldVendas"` | `ultima_venda_GoldVendas` é `null` |
| `"SilverSTGN_Dedup"` | `ultima_venda_SilverSTGN_Dedup` é `null` |

### `coletor_novo`

| Valor | Significa |
|-------|-----------|
| `"OK, sem registro"` | Farmácia sem pendência de envio |
| `"Pendente de envio no dia YYYY-MM-DD"` | Pendência registrada — a data indica quando foi capturada |
| `"Indisponível"` | Business Connect não respondeu (temporário) |

### `tipo_divergencia`

| Valor | Significa |
|-------|-----------|
| `null` | Sem divergência — datas iguais nas duas fontes |
| `"data_diferente"` | Presente nas duas fontes, mas com datas distintas |
| `"apenas_gold_vendas"` | Presente somente em `associacao.vendas` |
| `"apenas_silver_stgn_dedup"` | Presente somente em `silver.cadcvend_staging_dedup` |

---

### `POST /comparar` — Disparar nova comparação

Executa as queries no Redshift e salva o resultado. Use quando o usuário quiser atualizar os dados.

**URL:**
```
POST http://localhost:8000/comparar
Content-Type: application/json
```

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
  "divergencias": [ ... ],
  "status_farmacias": [ ... ]
}
```

> Após o POST, chame `GET /historico` (ou `/{associacao}`) e `GET /ultima-atualizacao` para atualizar a tela.

**Erros:**

| Código | Descrição |
|--------|-----------|
| `422` | `associacao` não informado |
| `503` | Falha de conexão com o Redshift |

**Exemplo JS:**
```js
async function atualizarComparacao(associacao) {
  const res = await fetch('http://localhost:8000/comparar', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ associacao }),
  });
  if (!res.ok) throw new Error((await res.json()).detail);
  return res.json();
}
```

---

### `GET /comparar` — Comparar via query param

Alternativa ao POST para chamadas diretas.

```
GET http://localhost:8000/comparar?associacao=80
```

---

## Outros endpoints

| Endpoint | Uso |
|----------|-----|
| `GET /` | Verifica se a API está no ar |
| `GET /health` | Status + conectividade com Redshift |
| `GET /docs` | Swagger UI interativo |

---

## CORS

A API já tem CORS habilitado (`Access-Control-Allow-Origin: *`).  
Chamadas diretas do browser funcionam sem configuração adicional.

Para restringir a origens específicas, ajuste `CORS_ORIGINS` no `.env`:
```
CORS_ORIGINS=http://localhost:3000,https://dashboard.suaempresa.com
```


---

## Fluxo recomendado para o dashboard

```
1. Ao abrir o frontend
   └─ GET /historico  →  tabela com TODAS as farmácias de TODAS as associações

2. Usuário quer filtrar por uma associação específica
   └─ GET /historico/{associacao}  →  mesma estrutura, só uma associação

3. Usuário dispara nova comparação (botão "Atualizar")
   └─ POST /comparar  →  executa queries no Redshift e atualiza o banco local
      └─ após o POST, recarregue GET /historico para refletir os novos dados
```

---

## Endpoints

### `GET /historico` — Todas as farmácias de todas as associações

Tela inicial: carrega automaticamente a tabela completa com todas as farmácias de todas as associações que possuem comparações salvas.

**URL:**
```
GET http://localhost:8000/historico
```

**Resposta (200):** array de farmácias (mesma estrutura de `/historico/{associacao}`, com campo `associacao` preenchido)

```json
[
  {
    "associacao": "80",
    "cod_farmacia": "30559",
    "nome_farmacia": "FRANQUIA PLANALTO",
    "cnpj": "12345678000199",
    "ultima_venda_GoldVendas": "2026-04-08",
    "ultima_hora_venda_GoldVendas": "2026-04-08 18:30:00",
    "ultima_venda_SilverSTGN_Dedup": "2026-04-14",
    "ultima_hora_venda_SilverSTGN_Dedup": "18:55:10",
    "coletor_novo": "Pendente de envio no dia 2026-04-10",
    "tipo_divergencia": "data_diferente"
  },
  {
    "associacao": "120",
    "cod_farmacia": "11111",
    "nome_farmacia": "FARMÁCIA CENTRAL",
    "cnpj": "98765432000100",
    "ultima_venda_GoldVendas": "2026-04-14",
    "ultima_hora_venda_GoldVendas": "2026-04-14 09:10:00",
    "ultima_venda_SilverSTGN_Dedup": "2026-04-14",
    "ultima_hora_venda_SilverSTGN_Dedup": "09:10:00",
    "coletor_novo": "OK, sem registro",
    "tipo_divergencia": null
  }
]
```

**Exemplo JS:**
```js
const res = await fetch('http://localhost:8000/historico');
const farmacias = await res.json();
// Renderiza tabela com todas as farmácias
// Para filtrar por associação no frontend: farmacias.filter(f => f.associacao === '80')
```

---

### `GET /historico/{associacao}` — Farmácias de uma associação específica

Retorna a tabela da **última comparação** de uma associação, útil para filtrar na tela.

**URL:**
```
GET http://localhost:8000/historico/80
```

**Resposta (200):**
```json
[
  {
    "cod_farmacia": "30559",
    "nome_farmacia": "FRANQUIA PLANALTO",
    "cnpj": "12345678000199",
    "ultima_venda_GoldVendas": "2026-04-08",
    "ultima_hora_venda_GoldVendas": "2026-04-08 18:30:00",
    "ultima_venda_SilverSTGN_Dedup": "2026-04-14",
    "ultima_hora_venda_SilverSTGN_Dedup": "18:55:10",
    "coletor_novo": "Pendente de envio no dia 2026-04-10",
    "tipo_divergencia": "data_diferente"
  },
  {
    "cod_farmacia": "24434",
    "nome_farmacia": "FARMÁCIA BOA SAÚDE",
    "cnpj": "98765432000100",
    "ultima_venda_GoldVendas": "2026-04-14",
    "ultima_hora_venda_GoldVendas": "2026-04-14 09:10:00",
    "ultima_venda_SilverSTGN_Dedup": "2026-04-14",
    "ultima_hora_venda_SilverSTGN_Dedup": "09:10:00",
    "coletor_novo": "OK, sem registro",
    "tipo_divergencia": null
  }
]
```

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `associacao` | string | Código da associação |
| `cod_farmacia` | string | Código da farmácia |
| `nome_farmacia` | string \| null | Nome da farmácia |
| `cnpj` | string \| null | CNPJ somente com dígitos (sem `.` `-` `/`) |
| `ultima_venda_GoldVendas` | string \| null | Última venda em `associacao.vendas` |
| `ultima_hora_venda_GoldVendas` | string \| null | Hora da última venda em `associacao.vendas` |
| `ultima_venda_SilverSTGN_Dedup` | string \| null | Última venda em `silver.cadcvend_staging_dedup` |
| `ultima_hora_venda_SilverSTGN_Dedup` | string \| null | Hora da última venda em `silver.cadcvend_staging_dedup` |
| `coletor_novo` | string \| null | Status no Business Connect (ver tabela abaixo) |
| `tipo_divergencia` | string \| null | Tipo de divergência — `null` se não há divergência |
| `camadas_atrasadas` | string[] \| null | Camadas com atraso detectado — `null` se não há atraso |

**Valores de `camadas_atrasadas`:**

| Valor | Camada | Critério |
|-------|--------|----------|
| `"GoldVendas"` | `associacao.vendas` | `ultima_venda_GoldVendas` anterior a D-1 (D-2 ou mais antigo) |
| `"SilverSTGN_Dedup"` | `silver.cadcvend_staging_dedup` | `ultima_venda_SilverSTGN_Dedup` anterior a D-1 (D-2 ou mais antigo) |
| `"API"` | Business Connect | `"Pendente de envio"` com data igual ou anterior a D-1 (ontem já conta) |

Exemplos:
- `["GoldVendas", "API"]` — atraso em Gold e no coletor
- `["SilverSTGN_Dedup"]` — atraso somente em Silver
- `null` — sem atraso em nenhuma camada

**Valores de `coletor_novo`:**

| Valor | Significa |
|-------|-----------|
| `"OK, sem registro"` | Farmácia sem pendência de envio |
| `"Pendente de envio no dia YYYY-MM-DD"` | Pendência registrada — a data indica quando foi capturada |
| `"Indisponível"` | Business Connect não respondeu (temporário) |

**Valores de `tipo_divergencia`:**

| Valor | Significa |
|-------|-----------|
| `null` | Sem divergência — datas iguais nas duas fontes |
| `"data_diferente"` | Presente nas duas fontes, mas com datas distintas |
| `"apenas_gold_vendas"` | Presente somente em `associacao.vendas` |
| `"apenas_silver_stgn_dedup"` | Presente somente em `silver.cadcvend_staging_dedup` |

**Erros:**

| Código | Descrição |
|--------|-----------|
| `404` | Nenhuma comparação encontrada para a associação |
| `503` | Erro ao acessar o banco local |

**Exemplo JS:**
```js
async function buscarDetalhes(associacao) {
  const res = await fetch(`http://localhost:8000/historico/${associacao}`);
  if (!res.ok) throw new Error(`Erro ${res.status}`);
  return res.json(); // array de farmácias
}
```

---

### `POST /comparar` — Disparar nova comparação

Executa as queries no Redshift e salva o resultado. Use quando o usuário quiser atualizar os dados.

**URL:**
```
POST http://localhost:8000/comparar
Content-Type: application/json
```

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
  "divergencias": [ ... ],
  "status_farmacias": [ ... ]
}
```

> Após o POST, chame `GET /historico` ou `GET /historico/{associacao}` para atualizar a tela.

**Erros:**

| Código | Descrição |
|--------|-----------|
| `422` | `associacao` não informado |
| `503` | Falha de conexão com o Redshift |

**Exemplo JS:**
```js
async function atualizarComparacao(associacao) {
  const res = await fetch('http://localhost:8000/comparar', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ associacao }),
  });
  if (!res.ok) throw new Error((await res.json()).detail);
  return res.json();
}
```

---

### `GET /comparar` — Comparar via query param

Alternativa ao POST para chamadas diretas.

```
GET http://localhost:8000/comparar?associacao=80
```

---

## Outros endpoints

| Endpoint | Uso |
|----------|-----|
| `GET /` | Verifica se a API está no ar |
| `GET /health` | Status + conectividade com Redshift |
| `GET /docs` | Swagger UI interativo |

---

## CORS

A API já tem CORS habilitado (`Access-Control-Allow-Origin: *`).  
Chamadas diretas do browser funcionam sem configuração adicional.

Para restringir a origens específicas, ajuste `CORS_ORIGINS` no `.env`:
```
CORS_ORIGINS=http://localhost:3000,https://dashboard.suaempresa.com
```


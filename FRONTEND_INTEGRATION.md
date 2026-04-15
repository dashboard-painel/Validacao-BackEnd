# Guia de Integração — Frontend

API rodando em `http://localhost:8000` (ajustar para o host de produção).

---

## Endpoint principal

### `GET /comparar` — via query params

Útil para links diretos ou chamadas simples.

**URL:**
```
GET http://localhost:8000/comparar?associacao={codigo}&dat_emissao={data}
```

**Parâmetros:**

| Parâmetro | Tipo | Obrigatório | Exemplo | Descrição |
|-----------|------|:-----------:|---------|-----------|
| `associacao` | string | ✅ | `80` | Código da associação |
| `dat_emissao` | string (YYYY-MM-DD) | ❌ | `2026-04-01` | Data mínima de emissão. **Default: 30 dias atrás** |

---

### `POST /comparar` — via body JSON

Ideal para formulários onde o usuário digita os parâmetros.

**URL:**
```
POST http://localhost:8000/comparar
Content-Type: application/json
```

**Body:**
```json
{
  "associacao": "80",
  "dat_emissao": "2026-04-01"
}
```

| Campo | Tipo | Obrigatório | Descrição |
|-------|------|:-----------:|-----------|
| `associacao` | string | ✅ | Código da associação |
| `dat_emissao` | string (YYYY-MM-DD) | ❌ | Data mínima. **Default: 30 dias atrás** |

> Ambos os endpoints retornam **a mesma estrutura de resposta**.

---

## Resposta de sucesso (`200 OK`)

```json
{
  "associacao": "80",
  "dat_emissao_filtro": "2026-03-16",
  "total_q1": 64,
  "total_q2": 74,
  "total_divergencias": 36,
  "comparacao_id": 1,
  "divergencias": [
    {
      "cod_farmacia": "30559",
      "nome_farmacia": "FRANQUIA PLANALTO",
      "ultima_venda_GoldVendas": "2026-04-08",
      "ultima_hora_venda_GoldVendas": "2026-04-08 18:30:00",
      "ultima_venda_SilverSTGN_Dedup": "2026-04-14",
      "ultima_hora_venda_SilverSTGN_Dedup": "18:55:10",
      "tipo_divergencia": "data_diferente"
    },
    {
      "cod_farmacia": "24434",
      "nome_farmacia": null,
      "ultima_venda_GoldVendas": null,
      "ultima_hora_venda_GoldVendas": null,
      "ultima_venda_SilverSTGN_Dedup": "2026-04-14",
      "ultima_hora_venda_SilverSTGN_Dedup": "19:39:00",
      "tipo_divergencia": "apenas_q2"
    }
  ]
}
```

### Campos da resposta

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `associacao` | string | Código da associação consultada |
| `dat_emissao_filtro` | string | Data usada como filtro (YYYY-MM-DD) |
| `total_q1` | number | Total de farmácias em `associacao.vendas` |
| `total_q2` | number | Total de farmácias em `silver.cadcvend_staging_dedup` |
| `total_divergencias` | number | Quantidade de divergências encontradas |
| `comparacao_id` | number | ID do registro salvo no histórico local |
| `divergencias` | array | Lista de divergências (pode ser vazia `[]`) |

### Campos de cada divergência

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `cod_farmacia` | string | Código da farmácia |
| `nome_farmacia` | string \| null | Nome da farmácia (disponível apenas via Q1) |
| `ultima_venda_GoldVendas` | string \| null | Última venda em `associacao.vendas` |
| `ultima_hora_venda_GoldVendas` | string \| null | Hora da última venda em `associacao.vendas` |
| `ultima_venda_SilverSTGN_Dedup` | string \| null | Última venda em `silver.cadcvend_staging_dedup` |
| `ultima_hora_venda_SilverSTGN_Dedup` | string \| null | Hora da última venda em `silver.cadcvend_staging_dedup` |
| `tipo_divergencia` | string | Tipo da divergência (ver abaixo) |

### Tipos de divergência

| Valor | Significa |
|-------|-----------|
| `data_diferente` | Farmácia presente nas duas fontes, mas com datas diferentes |
| `apenas_q1` | Farmácia presente somente em `associacao.vendas` |
| `apenas_q2` | Farmácia presente somente em `silver.cadcvend_staging_dedup` |

> **Dica de exibição:** use `tipo_divergencia` para colorir/filtrar as linhas na tabela do dashboard.

---

## Erros

| Código | Quando ocorre | `detail` |
|--------|---------------|----------|
| `422` | `associacao` não informado ou `dat_emissao` fora do formato YYYY-MM-DD | Mensagem do FastAPI com o campo inválido |
| `503` | Falha de conexão com o Redshift | `"Erro de conexão com o banco de dados..."` |

**Exemplo de erro 422:**
```json
{
  "detail": [
    {
      "type": "missing",
      "loc": ["query", "associacao"],
      "msg": "Field required"
    }
  ]
}
```

**Exemplo de erro 503:**
```json
{
  "detail": "Erro de conexão com o banco de dados. Tente novamente em alguns instantes. Detalhes: InterfaceError"
}
```

---

## Exemplos de chamada

**JavaScript (fetch) — POST:**
```js
async function buscarDivergencias(associacao, datEmissao) {
  const res = await fetch('http://localhost:8000/comparar', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ associacao, dat_emissao: datEmissao }),
  });

  if (!res.ok) {
    const err = await res.json();
    throw new Error(err.detail || `Erro ${res.status}`);
  }

  return res.json(); // → ComparacaoResponse
}

// Uso:
const resultado = await buscarDivergencias('80', '2026-04-01');
console.log(resultado.total_divergencias); // 36
console.log(resultado.divergencias);       // array
```

**JavaScript (fetch) — GET:**
```js
async function buscarDivergencias(associacao, datEmissao) {
  const params = new URLSearchParams({ associacao });
  if (datEmissao) params.append('dat_emissao', datEmissao);

  const res = await fetch(`http://localhost:8000/comparar?${params}`);

  if (!res.ok) {
    const err = await res.json();
    throw new Error(err.detail || `Erro ${res.status}`);
  }

  return res.json();
}
```

**Axios — POST:**
```js
const { data } = await axios.post('http://localhost:8000/comparar', {
  associacao: '80',
  dat_emissao: '2026-04-01',
});
```

---

## CORS

A API já tem CORS habilitado (`Access-Control-Allow-Origin: *`).  
Chamadas diretas do browser funcionam sem configuração adicional.

Para restringir a origens específicas, ajuste `CORS_ORIGINS` no `.env`:
```
CORS_ORIGINS=http://localhost:3000,https://dashboard.suaempresa.com
```

---

## Outros endpoints

| Endpoint | Uso |
|----------|-----|
| `GET /` | Verifica se a API está no ar |
| `GET /health` | Status + conectividade com Redshift |
| `GET /docs` | Swagger UI interativo |

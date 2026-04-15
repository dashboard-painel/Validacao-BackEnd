# Validacao-BackEnd

API em Python (FastAPI) para comparação de dados entre queries no Amazon Redshift.

## Requisitos

- Python 3.11+
- Acesso ao cluster Amazon Redshift

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
   # Edite .env com suas credenciais do Redshift
   ```

## Executando

```bash
uvicorn app.main:app --reload
```

A API estará disponível em `http://localhost:8000`

## Endpoints

- `GET /` — Informações da API
- `GET /health` — Health check (inclui status da conexão com Redshift)
- `GET /comparar?associacao=X` — Compara os dados entre as 2 queries pelo campo `dat_emissao`

## Variáveis de Ambiente

| Variável | Descrição | Obrigatória |
|----------|-----------|-------------|
| REDSHIFT_HOST | Endpoint do cluster Redshift | Sim |
| REDSHIFT_PORT | Porta (padrão: 5439) | Sim |
| REDSHIFT_DATABASE | Nome do banco de dados | Sim |
| REDSHIFT_USER | Usuário | Sim |
| REDSHIFT_PASSWORD | Senha | Sim |
| REDSHIFT_SCHEMA | Schema (padrão: public) | Não |

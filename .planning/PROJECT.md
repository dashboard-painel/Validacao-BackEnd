# Validacao-BackEnd

## What This Is

API em Python (FastAPI) que conecta ao Amazon Redshift, executa 2 queries parametrizadas e compara os resultados focando no campo `dat_emissao`. Retorna as divergências encontradas entre as duas consultas para consumo pelo frontend (dashboard já existente). O primeiro parâmetro implementado é `associacao`.

## Core Value

Detectar e expor divergências no campo `dat_emissao` entre duas consultas no Redshift de forma rápida e confiável, para que o frontend possa exibi-las ao usuário.

## Requirements

### Validated

(None yet — ship to validate)

### Active

- [ ] Endpoint FastAPI que recebe o parâmetro `associacao` e executa as 2 queries no Redshift
- [ ] Comparação dos resultados das 2 queries pelo campo `dat_emissao` para identificar divergências
- [ ] Retorno das linhas divergentes incluindo `hor_emissao` para visualização
- [ ] Conexão com Redshift via credenciais em variáveis de ambiente (.env)
- [ ] Resposta da API em formato JSON consumível pelo frontend

### Out of Scope

- Interface visual/frontend — já existe um dashboard separado
- Autenticação/autorização na API — não requerido na v1
- Persistência/histórico de comparações — fora do escopo inicial

## Context

- Já existe um dashboard (frontend) que consome a resposta da API
- Dois campos principais: `dat_emissao` (campo de comparação) e `hor_emissao` (exibição visual)
- Parâmetro inicial: `associacao` — outros parâmetros podem ser adicionados futuramente
- Stack: Python + FastAPI + psycopg2/redshift-connector + python-dotenv

## Constraints

- **Stack**: Python + FastAPI — definido pelo time
- **Banco**: Amazon Redshift — já em uso pela empresa
- **Config**: Credenciais via variáveis de ambiente (.env) — sem segredos no código

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| FastAPI como framework | Leveza, performance e fácil consumo pelo frontend | — Pending |
| Comparação por dat_emissao | Campo de negócio mais relevante para detectar divergências | — Pending |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-04-15 after initialization*

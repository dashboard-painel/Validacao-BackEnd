<!-- GSD:project-start source:PROJECT.md -->
## Project

**Validacao-BackEnd**

API em Python (FastAPI) que conecta ao Amazon Redshift, executa 2 queries parametrizadas e compara os resultados focando no campo `dat_emissao`. Retorna as divergências encontradas entre as duas consultas para consumo pelo frontend (dashboard já existente). O primeiro parâmetro implementado é `associacao`.

**Core Value:** Detectar e expor divergências no campo `dat_emissao` entre duas consultas no Redshift de forma rápida e confiável, para que o frontend possa exibi-las ao usuário.

### Constraints

- **Stack**: Python + FastAPI — definido pelo time
- **Banco**: Amazon Redshift — já em uso pela empresa
- **Config**: Credenciais via variáveis de ambiente (.env) — sem segredos no código
<!-- GSD:project-end -->

<!-- GSD:stack-start source:STACK.md -->
## Technology Stack

Technology stack not yet documented. Will populate after codebase mapping or first phase.
<!-- GSD:stack-end -->

<!-- GSD:conventions-start source:CONVENTIONS.md -->
## Conventions

Conventions not yet established. Will populate as patterns emerge during development.
<!-- GSD:conventions-end -->

<!-- GSD:architecture-start source:ARCHITECTURE.md -->
## Architecture

Architecture not yet mapped. Follow existing patterns found in the codebase.
<!-- GSD:architecture-end -->

<!-- GSD:skills-start source:skills/ -->
## Project Skills

No project skills found. Add skills to any of: `.github/skills/`, `.agents/skills/`, `.cursor/skills/`, or `.github/skills/` with a `SKILL.md` index file.
<!-- GSD:skills-end -->

<!-- GSD:workflow-start source:GSD defaults -->
## GSD Workflow Enforcement

Before using Edit, Write, or other file-changing tools, start work through a GSD command so planning artifacts and execution context stay in sync.

Use these entry points:
- `/gsd-quick` for small fixes, doc updates, and ad-hoc tasks
- `/gsd-debug` for investigation and bug fixing
- `/gsd-execute-phase` for planned phase work

Do not make direct repo edits outside a GSD workflow unless the user explicitly asks to bypass it.
<!-- GSD:workflow-end -->



<!-- GSD:profile-start -->
## Developer Profile

> Profile not yet configured. Run `/gsd-profile-user` to generate your developer profile.
> This section is managed by `generate-claude-profile` -- do not edit manually.
<!-- GSD:profile-end -->

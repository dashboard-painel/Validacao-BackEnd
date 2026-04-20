from app.clients.business_connect import buscar_status_farmacias as _buscar_status_farmacias
from app.local_db import (
    salvar_comparacao as _salvar_comparacao,
    salvar_status_farmacias as _salvar_status_farmacias,
    buscar_todos_consolidados as _buscar_todos_consolidados,
    buscar_historico_por_associacao as _buscar_historico_por_associacao,
    buscar_ultima_atualizacao as _buscar_ultima_atualizacao,
)
from app.clients.coletor_bi import buscar_por_codigo as _buscar_por_codigo


def buscar_status_farmacias(codigos: list[str]) -> dict:
    return _buscar_status_farmacias(codigos)


def salvar_comparacao(associacao: str, resultados_gold: list[dict], resultados_silver: list[dict], divergencias: list[dict]) -> int:
    return _salvar_comparacao(associacao, resultados_gold, resultados_silver, divergencias)


def salvar_status_farmacias(comparacao_id: int, associacao: str, status_dict: dict, coletor_bi: dict | None = None):
    _salvar_status_farmacias(comparacao_id, associacao, status_dict, coletor_bi)


def buscar_todos_consolidados():
    return _buscar_todos_consolidados()


def buscar_historico_por_associacao(associacao: str):
    return _buscar_historico_por_associacao(associacao)


def buscar_ultima_atualizacao():
    return _buscar_ultima_atualizacao()


def buscar_por_codigo(codigo: str):
    return _buscar_por_codigo(codigo)
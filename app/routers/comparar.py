"""Router para endpoint de comparação de dados."""
import asyncio
import functools
import logging

import psycopg2
import redshift_connector
from fastapi import APIRouter, HTTPException, Query

from app.repositories.comparacao_repository import (
    buscar_todos_consolidados,
    buscar_historico_por_associacao,
    buscar_ultima_atualizacao,
    # buscar_por_codigo,
)
from app.local_db import buscar_vendas_parceiros, buscar_ultima_atualizacao_vendas_parceiros

from app.mappers.comparacao_mapper import montar_resultado_consolidado, montar_vendas_parceiros_item
from app.schemas import ComparacaoRequest, ComparacaoResponse, ResultadoConsolidadoResponse, VendasParceirosResponse
from app.services.comparacao_service import executar_comparacao
from app.services.vendas_parceiros_service import executar_vendas_parceiros as executar_vp

logger = logging.getLogger(__name__)

router = APIRouter(tags=["comparação"])

# Limita a 1 comparação pesada por vez; requests extras aguardam na fila
_comparar_semaphore = asyncio.Semaphore(1)

# Exceções que indicam falha de conexão com banco de dados
_DB_ERRORS = (
    ConnectionError,
    psycopg2.OperationalError,
    redshift_connector.Error,
    OSError,
)


def _handle_errors(detail_503: str = "Erro ao acessar o banco de dados."):
    """Decorator que converte exceções em HTTPException (503 para DB, 500 para o resto)."""
    def _raise_for(fn_name: str, exc: Exception) -> None:
        if isinstance(exc, HTTPException):
            raise
        if isinstance(exc, _DB_ERRORS):
            logger.error("Erro de conexão em %s: %s: %s", fn_name, type(exc).__name__, exc)
            raise HTTPException(status_code=503, detail=detail_503)
        logger.error("Erro inesperado em %s: %s: %s", fn_name, type(exc).__name__, exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Erro interno do servidor.")

    def decorator(fn):
        if asyncio.iscoroutinefunction(fn):
            @functools.wraps(fn)
            async def async_wrapper(*args, **kwargs):
                try:
                    return await fn(*args, **kwargs)
                except Exception as e:
                    _raise_for(fn.__name__, e)
            return async_wrapper
        else:
            @functools.wraps(fn)
            def sync_wrapper(*args, **kwargs):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    _raise_for(fn.__name__, e)
            return sync_wrapper
    return decorator


@router.get("/comparar", response_model=ComparacaoResponse)
@_handle_errors("Erro de conexão com o banco de dados. Tente novamente em alguns instantes.")
async def comparar(
    associacao: str = Query(..., description="Código da associação para filtrar"),
) -> ComparacaoResponse:
    async with _comparar_semaphore:
        return await asyncio.to_thread(executar_comparacao, associacao)


@router.post("/comparar", response_model=ComparacaoResponse)
@_handle_errors("Erro de conexão com o banco de dados. Tente novamente em alguns instantes.")
async def comparar_post(body: ComparacaoRequest) -> ComparacaoResponse:
    async with _comparar_semaphore:
        return await asyncio.to_thread(executar_comparacao, body.associacao)


@router.get("/historico", response_model=list[ResultadoConsolidadoResponse])
@_handle_errors("Erro ao acessar o banco local.")
def listar_todas_farmacias() -> list[ResultadoConsolidadoResponse]:
    rows = buscar_todos_consolidados()
    return [montar_resultado_consolidado(row) for row in rows]


@router.get("/historico/{associacao}", response_model=list[ResultadoConsolidadoResponse])
@_handle_errors("Erro ao acessar o banco local.")
def historico_por_associacao(associacao: str) -> list[ResultadoConsolidadoResponse]:
    rows = buscar_historico_por_associacao(associacao)
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Nenhuma comparação encontrada para associacao={associacao}",
        )
    return [montar_resultado_consolidado(row) for row in rows]


@router.get("/ultima-atualizacao")
@_handle_errors("Erro ao acessar o banco local.")
def ultima_atualizacao() -> dict:
    return {"atualizado_em": buscar_ultima_atualizacao()}


@router.get("/coletor/{codigo}")
@_handle_errors("Erro ao acessar o coletor.")
def coletor_codigo(codigo: str) -> dict:
    return {"data_hora_ultima_venda": buscar_por_codigo(codigo)}


@router.get("/vendas-parceiros", response_model=VendasParceirosResponse)
@_handle_errors("Erro de conexão com o banco de dados. Tente novamente em alguns instantes.")
async def vendas_parceiros() -> VendasParceirosResponse:
    """Consulta vendas_parceiros no Redshift (JOIN com dimensao_cadastro_lojas) e persiste."""
    async with _comparar_semaphore:
        return await asyncio.to_thread(executar_vp)


@router.get("/vendas-parceiros/historico", response_model=VendasParceirosResponse)
@_handle_errors("Erro ao acessar o banco local.")
def vendas_parceiros_historico() -> VendasParceirosResponse:
    """Retorna vendas_parceiros do banco local (sem consultar Redshift)."""
    rows = buscar_vendas_parceiros()
    items = [montar_vendas_parceiros_item(row) for row in rows]
    return VendasParceirosResponse(total=len(items), resultados=items)


@router.get("/vendas-parceiros/ultima-atualizacao")
@_handle_errors("Erro ao acessar o banco local.")
def ultima_atualizacao_vendas_parceiros() -> dict:
    """Retorna a última atualização de vendas_parceiros no banco local."""
    return {"atualizado_em": buscar_ultima_atualizacao_vendas_parceiros()}

"""Router para endpoint de comparação de dados."""
import logging

from fastapi import APIRouter, HTTPException, Query

from app.repositories.comparacao_repository import (
    buscar_todos_consolidados,
    buscar_historico_por_associacao,
    buscar_ultima_atualizacao,
    buscar_por_codigo,
)

from app.mappers.comparacao_mapper import montar_resultado_consolidado
from app.schemas import ComparacaoRequest, ComparacaoResponse, ResultadoConsolidadoResponse
from app.services.comparacao_service import executar_comparacao

logger = logging.getLogger(__name__)

router = APIRouter(tags=["comparação"])

@router.get("/comparar", response_model=ComparacaoResponse)
def comparar(
    associacao: str = Query(..., description="Código da associação para filtrar"),
) -> ComparacaoResponse:
    try:
        return executar_comparacao(associacao)
    except Exception as e:
        logger.error("Erro ao executar comparação: %s: %s", type(e).__name__, e)
        raise HTTPException(
            status_code=503,
            detail=f"Erro de conexão com o banco de dados. Tente novamente em alguns instantes. Detalhes: {type(e).__name__}",
        )

@router.post("/comparar", response_model=ComparacaoResponse)
def comparar_post(body: ComparacaoRequest) -> ComparacaoResponse:
    try:
        return executar_comparacao(body.associacao)
    except Exception as e:
        logger.error("Erro ao executar comparação: %s: %s", type(e).__name__, e)
        raise HTTPException(
            status_code=503,
            detail=f"Erro de conexão com o banco de dados. Tente novamente em alguns instantes. Detalhes: {type(e).__name__}",
        )

@router.get("/historico", response_model=list[ResultadoConsolidadoResponse])
def listar_todas_farmacias() -> list[ResultadoConsolidadoResponse]:
    try:
        rows = buscar_todos_consolidados()
    except Exception as e:
        logger.error("Erro ao buscar todos consolidados: %s: %s", type(e).__name__, e)
        raise HTTPException(
            status_code=503,
            detail=f"Erro ao acessar o banco local. Detalhes: {type(e).__name__}",
        )

    return [montar_resultado_consolidado(row) for row in rows]

@router.get("/historico/{associacao}", response_model=list[ResultadoConsolidadoResponse])
def historico_por_associacao(associacao: str) -> list[ResultadoConsolidadoResponse]:
    try:
        rows = buscar_historico_por_associacao(associacao)
    except Exception as e:
        logger.error("Erro ao buscar historico para associacao=%s: %s: %s", associacao, type(e).__name__, e)
        raise HTTPException(
            status_code=503,
            detail=f"Erro ao acessar o banco local. Detalhes: {type(e).__name__}",
        )

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Nenhuma comparação encontrada para associacao={associacao}",
        )

    return [montar_resultado_consolidado(row) for row in rows]

@router.get("/ultima-atualizacao")
def ultima_atualizacao() -> dict:
    try:
        atualizado_em = buscar_ultima_atualizacao()
    except Exception as e:
        logger.error("Erro ao buscar ultima atualizacao: %s: %s", type(e).__name__, e)
        raise HTTPException(status_code=503, detail=f"Erro ao acessar o banco local. Detalhes: {type(e).__name__}")

    return {"atualizado_em": atualizado_em}

@router.get("/coletor/{codigo}")
def coletor_codigo(codigo: str) -> dict:
    try:
        data_hora_ultima_venda = buscar_por_codigo(codigo)
    except Exception as e:
        logger.error("Erro ao buscar status do coletor para código %s: %s: %s", codigo, type(e).__name__, e)
        raise HTTPException(status_code=503, detail=f"Erro ao acessar o coletor. Detalhes: {type(e).__name__}")

    return {"data_hora_ultima_venda": data_hora_ultima_venda}
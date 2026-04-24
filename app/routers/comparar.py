"""Router para endpoint de comparação de dados."""
import logging

from fastapi import APIRouter, HTTPException, Query

from app.repositories.comparacao_repository import (
    buscar_todos_consolidados,
    buscar_historico_por_associacao,
    buscar_ultima_atualizacao,
    buscar_por_codigo,
)
from app.local_db import buscar_vendas_parceiros, buscar_ultima_atualizacao_vendas_parceiros

from app.mappers.comparacao_mapper import montar_resultado_consolidado
from app.schemas import ComparacaoRequest, ComparacaoResponse, ResultadoConsolidadoResponse, VendasParceirosResponse, VendasParceirosItemResponse
from app.services.comparacao_service import executar_comparacao
from app.services.vendas_parceiros_service import executar_vendas_parceiros as executar_vp

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

@router.get("/vendas-parceiros", response_model=VendasParceirosResponse)
def vendas_parceiros() -> VendasParceirosResponse:
    """Consulta vendas_parceiros no Redshift (JOIN com dimensao_cadastro_lojas) e persiste."""
    try:
        return executar_vp()
    except Exception as e:
        logger.error("Erro ao executar vendas_parceiros: %s: %s", type(e).__name__, e)
        raise HTTPException(
            status_code=503,
            detail=f"Erro de conexão com o banco de dados. Tente novamente em alguns instantes. Detalhes: {type(e).__name__}",
        )

@router.get("/vendas-parceiros/historico", response_model=VendasParceirosResponse)
def vendas_parceiros_historico() -> VendasParceirosResponse:
    """Retorna vendas_parceiros do banco local (sem consultar Redshift)."""
    try:
        rows = buscar_vendas_parceiros()
        items = [
            VendasParceirosItemResponse(
                cod_farmacia=str(row["cod_farmacia"]).strip(),
                nome_farmacia=row.get("nome_farmacia"),
                sit_contrato=row.get("sit_contrato"),
                associacao=str(row.get("associacao", "")).strip(),
                farmacia=str(row["farmacia"]) if row.get("farmacia") is not None else None,
                associacao_parceiro=str(row["associacao_parceiro"]) if row.get("associacao_parceiro") else None,
                ultima_venda_parceiros=row.get("ultima_venda_parceiros"),
            )
            for row in rows
        ]
        return VendasParceirosResponse(total=len(items), resultados=items)
    except Exception as e:
        logger.error("Erro ao buscar histórico vendas_parceiros: %s: %s", type(e).__name__, e)
        raise HTTPException(
            status_code=503,
            detail=f"Erro ao acessar o banco local. Detalhes: {type(e).__name__}",
        )

@router.get("/vendas-parceiros/ultima-atualizacao")
def ultima_atualizacao_vendas_parceiros() -> dict:
    """Retorna a última atualização de vendas_parceiros no banco local."""
    try:
        atualizado_em = buscar_ultima_atualizacao_vendas_parceiros()
    except Exception as e:
        logger.error("Erro ao buscar ultima atualizacao vendas_parceiros: %s: %s", type(e).__name__, e)
        raise HTTPException(status_code=503, detail=f"Erro ao acessar o banco local. Detalhes: {type(e).__name__}")

    return {"atualizado_em": atualizado_em}
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
    """Compara os resultados das duas queries do Redshift.

    Executa GoldVendas (associacao.vendas) e SilverSTGN_Dedup (silver.cadcvend_staging_dedup)
    e retorna a última venda registrada por farmácia, sem filtro de data.

    Tipos de divergência identificados:
    - **data_diferente**: farmácia presente em ambas mas com datas distintas
    - **apenas_gold_vendas**: farmácia presente somente em GoldVendas
    - **apenas_silver_stgn_dedup**: farmácia presente somente em SilverSTGN_Dedup

    Args:
        associacao: Código da associação (obrigatório)

    Returns:
        ComparacaoResponse com totais e lista de divergências

    Raises:
        HTTPException 503: Erro de conexão com o Redshift
    """
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
    """Compara os resultados das duas queries via body JSON.

    Mesma lógica do GET /comparar, mas recebe os parâmetros no corpo da requisição.

    Args:
        body: JSON com `associacao` (obrigatório)

    Returns:
        ComparacaoResponse com totais e lista de divergências

    Raises:
        HTTPException 503: Erro de conexão com o Redshift
    """
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
    """Retorna todas as farmácias de todas as associações (última comparação de cada uma).

    Ideal para a tela inicial do dashboard: carrega a tabela completa sem precisar
    especificar uma associação.

    Returns:
        Lista de todas as farmácias, ordenada por associacao e cod_farmacia,
        com coletor_novo e tipo_divergencia embutidos

    Raises:
        HTTPException 503: Erro de conexão com o banco local
    """
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
    """Retorna os resultados consolidados da comparação mais recente de uma associação.

    Inclui dados de ambas as fontes (GoldVendas + SilverSTGN_Dedup), status do
    coletor no Business Connect e tipo de divergência por farmácia.

    Args:
        associacao: Código da associação

    Returns:
        Lista de farmácias com dados consolidados, coletor_novo e tipo_divergencia

    Raises:
        HTTPException 404: Nenhuma comparação encontrada para a associação
        HTTPException 503: Erro de conexão com o banco local
    """
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
    """Retorna a data/hora da comparacao mais recente entre todas as associacoes."""
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
"""Router para endpoint de comparação de dados."""
import logging
import time

from fastapi import APIRouter, HTTPException, Query

from app.business_connect import buscar_status_farmacias
from app.comparador import comparar_resultados
from app.local_db import buscar_resultados_consolidados, salvar_status_farmacias
from app.schemas import ComparacaoRequest, ComparacaoResponse, DivergenciaResponse, FarmaciaStatusResponse, ResultadoConsolidadoResponse

logger = logging.getLogger(__name__)

router = APIRouter(tags=["comparação"])


@router.get("/comparar", response_model=ComparacaoResponse)
async def comparar(
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
    """
    return _executar_comparacao(associacao)


def _executar_comparacao(associacao: str) -> ComparacaoResponse:
    """Lógica compartilhada entre GET e POST.

    Fluxo:
    1. Executa comparação Redshift via comparar_resultados()
    2. Consulta status de migração no Business Connect para TODAS as farmácias
    3. Persiste status no banco local (se comparação foi salva)
    4. Retorna resposta unificada
    """
    logger.info("📥 Requisição recebida — associacao=%s", associacao)
    t_req = time.perf_counter()

    # 1. Comparação Redshift
    try:
        resultado = comparar_resultados(associacao)
    except Exception as e:
        logger.error("Erro ao executar comparação: %s: %s", type(e).__name__, e)
        raise HTTPException(
            status_code=503,
            detail=f"Erro de conexão com o banco de dados. Tente novamente em alguns instantes. Detalhes: {type(e).__name__}",
        )

    # 2. Business Connect — status de migração de todas as farmácias (GoldVendas + SilverSTGN_Dedup)
    logger.info("⏳ Aguardando resposta Business Connect — %d farmácias...", len(resultado.todas_farmacias))
    t_bc = time.perf_counter()
    try:
        status_dict = buscar_status_farmacias(list(resultado.todas_farmacias))
        logger.info("✅ Business Connect respondeu em %.2fs", time.perf_counter() - t_bc)
    except Exception as e:
        logger.warning(
            "Business Connect indisponível (%.2fs) — usando fallback 'Indisponível': %s: %s",
            time.perf_counter() - t_bc,
            type(e).__name__,
            e,
        )
        status_dict = {cod: "Indisponível" for cod in resultado.todas_farmacias}

    # 3. Persistir status no banco local
    if resultado.comparacao_id is not None:
        try:
            salvar_status_farmacias(resultado.comparacao_id, status_dict)
        except Exception as e:
            logger.warning(
                "Erro ao salvar status_farmacias no banco local (não crítico): %s: %s",
                type(e).__name__,
                e,
            )

    # 4. Construir resposta
    divergencias_response = [
        DivergenciaResponse(
            cod_farmacia=d.cod_farmacia,
            nome_farmacia=d.nome_farmacia,
            cnpj=d.cnpj,
            ultima_venda_GoldVendas=d.ultima_venda_GoldVendas,
            ultima_hora_venda_GoldVendas=d.ultima_hora_venda_GoldVendas,
            ultima_venda_SilverSTGN_Dedup=d.ultima_venda_SilverSTGN_Dedup,
            ultima_hora_venda_SilverSTGN_Dedup=d.ultima_hora_venda_SilverSTGN_Dedup,
            tipo_divergencia=d.tipo_divergencia,
        )
        for d in resultado.divergencias
    ]

    status_farmacias_response = [
        FarmaciaStatusResponse(cod_farmacia=cod, coletor_novo=status)
        for cod, status in status_dict.items()
    ]

    response = ComparacaoResponse(
        associacao=resultado.associacao,
        total_gold_vendas=resultado.total_gold_vendas,
        total_silver_stgn_dedup=resultado.total_silver_stgn_dedup,
        total_divergencias=resultado.total_divergencias,
        comparacao_id=resultado.comparacao_id,
        divergencias=divergencias_response,
        status_farmacias=status_farmacias_response,
    )
    logger.info("🚀 Requisição concluída em %.2fs", time.perf_counter() - t_req)
    return response


@router.post("/comparar", response_model=ComparacaoResponse)
async def comparar_post(body: ComparacaoRequest) -> ComparacaoResponse:
    """Compara os resultados das duas queries via body JSON.

    Mesma lógica do GET /comparar, mas recebe os parâmetros no corpo da requisição.

    Args:
        body: JSON com `associacao` (obrigatório)

    Returns:
        ComparacaoResponse com totais e lista de divergências

    Raises:
        HTTPException 503: Erro de conexão com o Redshift
    """
    return _executar_comparacao(body.associacao)


@router.get("/historico/{comparacao_id}/consolidado", response_model=list[ResultadoConsolidadoResponse])
async def historico_consolidado(comparacao_id: int) -> list[ResultadoConsolidadoResponse]:
    """Retorna os resultados consolidados (Q1 + Q2 lado a lado) de uma comparação salva.

    Lê a tabela `resultados_consolidados` do banco local, que contém o FULL OUTER JOIN
    de GoldVendas (Q1) e SilverSTGN_Dedup (Q2) por farmácia.

    Args:
        comparacao_id: ID da comparação (retornado em `comparacao_id` da resposta de /comparar)

    Returns:
        Lista de registros com dados de ambas as fontes por farmácia

    Raises:
        HTTPException 404: Comparação não encontrada
        HTTPException 503: Erro de conexão com o banco local
    """
    try:
        rows = buscar_resultados_consolidados(comparacao_id)
    except Exception as e:
        logger.error("Erro ao buscar resultados_consolidados: %s: %s", type(e).__name__, e)
        raise HTTPException(
            status_code=503,
            detail=f"Erro ao acessar o banco local. Detalhes: {type(e).__name__}",
        )

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Nenhum resultado consolidado encontrado para comparacao_id={comparacao_id}",
        )

    return [
        ResultadoConsolidadoResponse(
            cod_farmacia=row["cod_farmacia"],
            nome_farmacia=row.get("nome_farmacia"),
            cnpj=row.get("cnpj"),
            ultima_venda_GoldVendas=row.get("ultima_venda_goldvendas"),
            ultima_hora_venda_GoldVendas=row.get("ultima_hora_venda_goldvendas"),
            ultima_venda_SilverSTGN_Dedup=row.get("ultima_venda_silverstgn_dedup"),
            ultima_hora_venda_SilverSTGN_Dedup=row.get("ultima_hora_venda_silverstgn_dedup"),
        )
        for row in rows
    ]

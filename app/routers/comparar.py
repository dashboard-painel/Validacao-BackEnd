"""Router para endpoint de comparação de dados."""
import logging
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.business_connect import buscar_status_farmacias
from app.comparador import comparar_resultados
from app.local_db import buscar_resultados_consolidados, salvar_status_farmacias
from app.schemas import ComparacaoRequest, ComparacaoResponse, DivergenciaResponse, FarmaciaStatusResponse, ResultadoConsolidadoResponse

logger = logging.getLogger(__name__)

router = APIRouter(tags=["comparação"])


def _default_dat_emissao() -> str:
    """Retorna data de 30 dias atrás no formato YYYY-MM-DD."""
    return (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")


@router.get("/comparar", response_model=ComparacaoResponse)
async def comparar(
    associacao: str = Query(..., description="Código da associação para filtrar"),
    dat_emissao: Optional[str] = Query(
        None,
        description="Data mínima de emissão (YYYY-MM-DD). Default: 30 dias atrás",
        pattern=r"^\d{4}-\d{2}-\d{2}$",
    ),
) -> ComparacaoResponse:
    """Compara os resultados das duas queries do Redshift.

    Executa GoldVendas (associacao.vendas) e SilverSTGN_Dedup (silver.cadcvend_staging_dedup)
    com os parâmetros fornecidos e retorna as divergências encontradas no campo
    ultima_venda (dat_emissao).

    Tipos de divergência identificados:
    - **data_diferente**: farmácia presente em ambas mas com datas distintas
    - **apenas_gold_vendas**: farmácia presente somente em GoldVendas
    - **apenas_silver_stgn_dedup**: farmácia presente somente em SilverSTGN_Dedup

    Args:
        associacao: Código da associação (obrigatório)
        dat_emissao: Data mínima no formato YYYY-MM-DD (opcional, default 30 dias)

    Returns:
        ComparacaoResponse com totais e lista de divergências
    """
    return _executar_comparacao(associacao, dat_emissao)


def _executar_comparacao(associacao: str, dat_emissao: Optional[str]) -> ComparacaoResponse:
    """Lógica compartilhada entre GET e POST.

    Fluxo:
    1. Executa comparação Redshift via comparar_resultados()
    2. Consulta status de migração no Business Connect para TODAS as farmácias
    3. Persiste status no banco local (se comparação foi salva)
    4. Retorna resposta unificada
    """
    data_filtro = dat_emissao if dat_emissao else _default_dat_emissao()

    # 1. Comparação Redshift
    try:
        resultado = comparar_resultados(associacao, data_filtro)
    except Exception as e:
        logger.error("Erro ao executar comparação: %s: %s", type(e).__name__, e)
        raise HTTPException(
            status_code=503,
            detail=f"Erro de conexão com o banco de dados. Tente novamente em alguns instantes. Detalhes: {type(e).__name__}",
        )

    # 2. Business Connect — status de migração de todas as farmácias (GoldVendas + SilverSTGN_Dedup)
    try:
        status_dict = buscar_status_farmacias(list(resultado.todas_farmacias))
    except Exception as e:
        logger.warning(
            "Business Connect indisponível — usando fallback 'Indisponível': %s: %s",
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

    return ComparacaoResponse(
        associacao=resultado.associacao,
        dat_emissao_filtro=resultado.dat_emissao_filtro,
        total_gold_vendas=resultado.total_gold_vendas,
        total_silver_stgn_dedup=resultado.total_silver_stgn_dedup,
        total_divergencias=resultado.total_divergencias,
        comparacao_id=resultado.comparacao_id,
        divergencias=divergencias_response,
        status_farmacias=status_farmacias_response,
    )


@router.post("/comparar", response_model=ComparacaoResponse)
async def comparar_post(body: ComparacaoRequest) -> ComparacaoResponse:
    """Compara os resultados das duas queries via body JSON.

    Mesma lógica do GET /comparar, mas recebe os parâmetros no corpo da requisição.
    Ideal para formulários onde o usuário digita a associação e a data.

    Args:
        body: JSON com `associacao` (obrigatório) e `dat_emissao` (opcional)

    Returns:
        ComparacaoResponse com totais e lista de divergências

    Raises:
        HTTPException 503: Erro de conexão com o Redshift
    """
    return _executar_comparacao(body.associacao, body.dat_emissao)


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
            ultima_venda_GoldVendas=row.get("ultima_venda_goldvendas"),
            ultima_hora_venda_GoldVendas=row.get("ultima_hora_venda_goldvendas"),
            ultima_venda_SilverSTGN_Dedup=row.get("ultima_venda_silverstgn_dedup"),
            ultima_hora_venda_SilverSTGN_Dedup=row.get("ultima_hora_venda_silverstgn_dedup"),
        )
        for row in rows
    ]

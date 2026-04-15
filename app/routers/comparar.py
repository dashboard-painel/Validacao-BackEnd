"""Router para endpoint de comparação de dados."""
import logging
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.business_connect import buscar_status_farmacias
from app.comparador import comparar_resultados
from app.local_db import salvar_status_farmacias
from app.schemas import ComparacaoRequest, ComparacaoResponse, DivergenciaResponse, FarmaciaStatusResponse

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

    Executa Query 1 (associacao.vendas) e Query 2 (silver.cadcvend_staging_dedup)
    com os parâmetros fornecidos e retorna as divergências encontradas no campo
    ultima_venda (dat_emissao).

    Tipos de divergência identificados:
    - **data_diferente**: farmácia presente em ambas mas com datas distintas
    - **apenas_q1**: farmácia presente somente na Query 1
    - **apenas_q2**: farmácia presente somente na Query 2

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

    # 2. Business Connect — status de migração de todas as farmácias (q1 + q2)
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
        total_q1=resultado.total_q1,
        total_q2=resultado.total_q2,
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

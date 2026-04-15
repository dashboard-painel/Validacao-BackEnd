"""Router para endpoint de comparação de dados."""
import logging
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.comparador import comparar_resultados
from app.schemas import ComparacaoRequest, ComparacaoResponse, DivergenciaResponse

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
    """Lógica compartilhada entre GET e POST."""
    data_filtro = dat_emissao if dat_emissao else _default_dat_emissao()

    try:
        resultado = comparar_resultados(associacao, data_filtro)
    except Exception as e:
        logger.error("Erro ao executar comparação: %s: %s", type(e).__name__, e)
        raise HTTPException(
            status_code=503,
            detail=f"Erro de conexão com o banco de dados. Tente novamente em alguns instantes. Detalhes: {type(e).__name__}",
        )

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

    return ComparacaoResponse(
        associacao=resultado.associacao,
        dat_emissao_filtro=resultado.dat_emissao_filtro,
        total_q1=resultado.total_q1,
        total_q2=resultado.total_q2,
        total_divergencias=resultado.total_divergencias,
        comparacao_id=resultado.comparacao_id,
        divergencias=divergencias_response,
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

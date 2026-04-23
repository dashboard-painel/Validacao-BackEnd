"""Service dedicado para consulta e persistência de vendas_parceiros."""
import logging
import time

from app.repositories.redshift_repository import execute_vendas_parceiros
from app.local_db import salvar_vendas_parceiros
from app.schemas import VendasParceirosItemResponse, VendasParceirosResponse

logger = logging.getLogger(__name__)


def executar_vendas_parceiros(associacao: str) -> VendasParceirosResponse:
    """Executa query vendas_parceiros no Redshift, persiste e retorna resultado."""
    logger.info("📥 Vendas Parceiros iniciada — associacao=%s", associacao)
    t0 = time.perf_counter()

    resultados = execute_vendas_parceiros(associacao)

    # Persistência
    try:
        salvar_vendas_parceiros(associacao, resultados)
        logger.info("💾 Vendas Parceiros persistidas — %d registros", len(resultados))
    except Exception as e:
        logger.warning("Erro ao salvar vendas_parceiros (não crítico): %s: %s", type(e).__name__, e)

    # Montar response
    items = [
        VendasParceirosItemResponse(
            cod_farmacia=str(r["cod_farmacia"]).strip(),
            nome_farmacia=r.get("nome_farmacia"),
            sit_contrato=r.get("sit_contrato"),
            associacao=str(r.get("associacao", associacao)).strip(),
            farmacia=r.get("farmacia"),
            associacao_parceiro=str(r["associacao_parceiro"]) if r.get("associacao_parceiro") else None,
            ultima_venda_parceiros=str(r["ultima_venda_parceiros"]) if r.get("ultima_venda_parceiros") else None,
        )
        for r in resultados
    ]

    logger.info("🚀 Vendas Parceiros finalizada em %.2fs — %d registros", time.perf_counter() - t0, len(items))
    return VendasParceirosResponse(
        associacao=associacao,
        total=len(items),
        resultados=items,
    )

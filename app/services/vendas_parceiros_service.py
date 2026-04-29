import logging
import time

from app.repositories.redshift_repository import execute_vendas_parceiros
from app.local_db import salvar_vendas_parceiros
from app.mappers.comparacao_mapper import montar_vendas_parceiros_item
from app.schemas import VendasParceirosResponse

logger = logging.getLogger(__name__)


def executar_vendas_parceiros() -> VendasParceirosResponse:

    logger.info("📥 Vendas Parceiros iniciada — buscando todas as redes")
    t0 = time.perf_counter()

    resultados = execute_vendas_parceiros()

    try:
        salvar_vendas_parceiros(resultados)
        logger.info("💾 Vendas Parceiros persistidas — %d registros", len(resultados))
    except Exception as e:
        logger.warning("Erro ao salvar vendas_parceiros (não crítico): %s: %s", type(e).__name__, e)

    # Montar response
    items = [montar_vendas_parceiros_item(r) for r in resultados]

    logger.info("🚀 Vendas Parceiros finalizada em %.2fs — %d registros", time.perf_counter() - t0, len(items))
    return VendasParceirosResponse(
        total=len(items),
        resultados=items,
    )

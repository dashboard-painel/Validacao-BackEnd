"""Utilitários de domínio compartilhados entre camadas."""
import logging
from datetime import date, timedelta
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


def camadas_atrasadas(
    data_gold: Optional[str],
    data_silver: Optional[str],
    coletor_novo: Optional[str],
) -> Tuple[Optional[list[str]], Optional[list[str]]]:

    ontem = date.today() - timedelta(days=1)
    atrasadas: list[str] = []
    sem_dados: list[str] = []

    for camada, d_str in [("GoldVendas", data_gold), ("SilverSTGN_Dedup", data_silver)]:
        if not d_str:
            sem_dados.append(camada)
        else:
            try:
                if date.fromisoformat(str(d_str)[:10]) < ontem:
                    atrasadas.append(camada)
            except ValueError:
                logger.warning("Data inválida ignorada para camada %s: %r", camada, d_str)

    if coletor_novo and coletor_novo.startswith("Pendente de envio no dia "):
        data_api = coletor_novo.removeprefix("Pendente de envio no dia ").strip()
        try:
            if datetime.strptime(data_api, "%d/%m/%Y %H:%M:%S").date() < ontem:
                atrasadas.append("API")
        except ValueError:
            logger.warning("Data inválida ignorada para camada API: %r", data_api)

    return (atrasadas or None, sem_dados or None)

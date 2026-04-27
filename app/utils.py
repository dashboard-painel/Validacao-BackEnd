"""Utilitários de domínio compartilhados entre camadas."""
from datetime import date, datetime, timedelta
from typing import Optional, Tuple


def camadas_atrasadas(
    data_gold: Optional[str],
    data_silver: Optional[str],
    coletor_novo: Optional[str],
) -> Tuple[Optional[list[str]], Optional[list[str]]]:
    """Retorna (camadas_atrasadas, camadas_sem_dados).

    camadas_atrasadas — tem dado mas é velho (data < D-1):
    - GoldVendas, SilverSTGN_Dedup, API

    camadas_sem_dados — sem nenhum registro:
    - GoldVendas, SilverSTGN_Dedup (campo null)
    """
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
                pass

    if coletor_novo and coletor_novo.startswith("Pendente de envio no dia "):
        data_api = coletor_novo.removeprefix("Pendente de envio no dia ").strip()
        try:
            if datetime.strptime(data_api, "%d/%m/%Y %H:%M:%S").date() < ontem:
                atrasadas.append("API")
        except ValueError:
            pass

    return (atrasadas or None, sem_dados or None)

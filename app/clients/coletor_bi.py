import os
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import time
import requests
import logging
import dotenv

dotenv.load_dotenv()

COLETOR_URL = os.getenv("COLETOR_URL")
COLETOR_USERNAME = os.getenv("COLETOR_USERNAME")
COLETOR_PASSWORD = os.getenv("COLETOR_PASSWORD")

logger = logging.getLogger(__name__)

def buscar_por_codigo(codigo: str) -> dict:
    url = f"{COLETOR_URL}/{codigo}-dados_vendas"

    try:
        response = requests.get(
            url,
            auth=(COLETOR_USERNAME, COLETOR_PASSWORD),
            timeout=30
        )

    except Exception:
        logger.warning("Coletor BI request falhou para farmácia %s: timeout ou erro de conexão", codigo)
        return {
            "farmacia": codigo,
            "ultima_data": None,
            "ultima_hora": None
        }

    if (response.status_code != 200):
        logger.warning(
            "Coletor BI request falhou para farmácia %s: HTTP %s",
            codigo,
            response.status_code,
        )
        return {
            "farmacia": codigo,
            "ultima_data": None,
            "ultima_hora": None
        }

    dados = response.json()

    resultados = [
        item for item in dados
        if isinstance(item, dict)
        and item.get("dat_emissao")
        and item.get("hor_emissao")
    ]

    if not resultados:
        return {
            "farmacia": codigo,
            "ultima_data": None,
            "ultima_hora": None
        }

    ultimo_registro = max(
        resultados,
        key=lambda x: datetime.strptime(
            f"{x['dat_emissao']} {x['hor_emissao']}",
            "%Y-%m-%d %H:%M:%S"
        )
    )
    return {
        "farmacia": codigo,
        "ultima_data": ultimo_registro["dat_emissao"],
        "ultima_hora": ultimo_registro["hor_emissao"]
    }

def buscar_por_associacao(codigos: list[str]) -> dict[str, dict]:

    if not codigos:
        return {}

    t_auth = time.perf_counter()

    logger.info("⏳ Consultando status no Coletor BI para %d farmácias...", len(codigos))

    resultado: dict[str, dict] = {}

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(buscar_por_codigo, codigo): codigo
            for codigo in codigos
        }

        for future in as_completed(futures):

            cod = futures[future]

            try:
                resultado[cod] = future.result()

            except Exception as e:
                logger.warning("Coletor BI request falhou para farmácia %s: %s", cod, e)

    logger.info("✅ Coletor BI consultado em %.2fs", time.perf_counter() - t_auth)

    return resultado
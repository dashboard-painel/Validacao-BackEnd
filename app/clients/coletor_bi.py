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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def buscar_por_codigo(codigos: str) -> str | None:
    url = f"{COLETOR_URL}/{codigos}-dados_vendas"

    t_auth = time.perf_counter()

    response = requests.get(
        url,
        auth=(COLETOR_USERNAME, COLETOR_PASSWORD),
        timeout=30
    )

    logger.info("⏳ Autenticando no Coletor BI...")

    if (response.status_code != 200):
        logger.warning(
            "Coletor BI request falhou para farmácia %s: HTTP %s",
            codigos,
            response.status_code,
        )
        return

    logger.info("✅ Coletor BI autenticado em %.2fs — consultando %d farmácias...", time.perf_counter() - t_auth,
                len(codigos))

    dados = response.json()

    datas = [
        item["dat_emissao"]
        for item in dados
        if isinstance(item, dict) and item.get("dat_emissao")
    ]

    horas = [
        item["hor_emissao"]
        for item in dados
        if isinstance(item, dict) and item.get("hor_emissao")
    ]

    if not datas or not horas:
        return f"Farmácia: {codigos} sem dados de vendas no Coletor BI"

    ultima_data = max(
        datas,
        key=lambda d: datetime.fromisoformat(d.replace("Z", "+00:00"))
    )

    ultimahora = max(
        horas,
        key=lambda h: datetime.strptime(h, "%H:%M:%S").time()
    )

    return f"{ultima_data} {ultimahora}"

def buscar_status_farmacias(codigos: list[str]) -> dict[str, str]:

    if not codigos:
        return {}

    t_auth = time.perf_counter()

    logger.info("⏳ Consultando status no Coletor BI para %d farmácias...", len(codigos))

    resultado: dict[str, str] = {}

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(buscar_por_codigo, codigos): codigos for codigos in codigos}

        for future in as_completed(futures):

            cod = futures[future]

            try:
                resultado[cod] = future.result()

            except Exception as e:
                logger.warning("Coletor BI request falhou para farmácia %s: %s", codigos, e)
                resultado[cod] = f"Farmácia: {codigos} sem dados de vendas no Coletor BI"

    logger.info("✅ Coletor BI consultado em %.2fs", time.perf_counter() - t_auth)
    return resultado
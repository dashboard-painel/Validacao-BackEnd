"""Módulo de integração com a API Coletor BI para última venda por farmácia."""
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests

COLETOR_URL = os.getenv("COLETOR_URL")
COLETOR_USERNAME = os.getenv("COLETOR_USERNAME")
COLETOR_PASSWORD = os.getenv("COLETOR_PASSWORD")

logger = logging.getLogger(__name__)


def buscar_por_codigo(codigo: str) -> dict:
    """Consulta a última venda de uma farmácia no Coletor BI.

    Args:
        codigo: Código da farmácia a consultar.

    Returns:
        Dict com chaves: farmacia, ultima_data, ultima_hora.
    """
    if not COLETOR_URL:
        return {"farmacia": codigo, "ultima_data": None, "ultima_hora": None}

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

    if response.status_code != 200:
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

    try:
        dados = response.json()
    except Exception:
        logger.warning("Coletor BI resposta inválida (JSON) para farmácia %s", codigo)
        return {"farmacia": codigo, "ultima_data": None, "ultima_hora": None}

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

    def _parse_dt(item):
        try:
            return datetime.strptime(
                f"{item['dat_emissao']} {item['hor_emissao']}",
                "%Y-%m-%d %H:%M:%S"
            )
        except (ValueError, KeyError):
            return datetime.min

    ultimo_registro = max(resultados, key=_parse_dt)
    return {
        "farmacia": codigo,
        "ultima_data": ultimo_registro["dat_emissao"],
        "ultima_hora": ultimo_registro["hor_emissao"]
    }


def buscar_por_associacao(codigos: list[str], executor: ThreadPoolExecutor | None = None) -> dict[str, dict]:
    """Consulta status no Coletor BI para múltiplas farmácias em paralelo.

    Args:
        codigos: Lista de códigos de farmácia a consultar
        executor: ThreadPoolExecutor compartilhado (opcional). Se não fornecido, cria um local.

    Returns:
        dict[str, dict]: Mapeamento {cod_farmacia: {farmacia, ultima_data, ultima_hora}}
    """
    if not codigos:
        return {}

    t_auth = time.perf_counter()
    logger.info("⏳ Consultando status no Coletor BI para %d farmácias...", len(codigos))

    resultado: dict[str, dict] = {}

    def _run(exec: ThreadPoolExecutor):
        futures = {exec.submit(buscar_por_codigo, codigo): codigo for codigo in codigos}
        for future in as_completed(futures):
            cod = futures[future]
            try:
                resultado[cod] = future.result()
            except Exception as e:
                logger.warning("Coletor BI request falhou para farmácia %s: %s", cod, e)

    if executor:
        _run(executor)
    else:
        with ThreadPoolExecutor(max_workers=3) as local_exec:
            _run(local_exec)

    logger.info("✅ Coletor BI consultado em %.2fs", time.perf_counter() - t_auth)

    return resultado

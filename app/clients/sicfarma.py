import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import dotenv
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

dotenv.load_dotenv()

SICFARMA_URL = os.getenv("SICFARMA_URL")
SICFARMA_USERNAME = os.getenv("SICFARMA_USERNAME")
SICFARMA_PASSWORD = os.getenv("SICFARMA_PASSWORD")

logger = logging.getLogger(__name__)

COD_SISTEMA_COLETOR = 21

_RETRY_CONFIG = Retry(
    total=3,
    read=1,
    backoff_factor=1.0,
    status_forcelist={500, 502, 503, 504},
    allowed_methods={"GET"},
    raise_on_status=False,
)

_SESSION = requests.Session()
_SESSION.mount("https://", HTTPAdapter(max_retries=_RETRY_CONFIG, pool_connections=1, pool_maxsize=25))
_SESSION.mount("http://", HTTPAdapter(max_retries=_RETRY_CONFIG, pool_connections=1, pool_maxsize=25))


def _get_with_retry(url: str, **kwargs) -> requests.Response:

    return _SESSION.get(url, **kwargs)

_SICFARMA_CLASSIFICATION_MAP: dict[int, str] = {
    1: "GOLD",
    3: "SELECT1",
    4: "SELECT2",
    5: "PRIME",
    7: "SNGPC",
    8: "INATIVO",
    9: "NEONATAL",
    10: "IMPLANTACAO",
    11: "100% BRASIL",
    16: "CLOUD",
    19: "NEONATAL CLOUD",
}


def buscar_classificacao_por_codigo(cod_farmacia: str) -> str | None:

    try:
        response = _get_with_retry(
            SICFARMA_URL,
            params={"id": cod_farmacia},
            auth=(SICFARMA_USERNAME, SICFARMA_PASSWORD),
            timeout=10,
        )
    except Exception as e:
        logger.warning(
            "Sicfarma request falhou para farmácia %s: %s", cod_farmacia, e
        )
        return None

    if response.status_code != 200:
        logger.warning(
            "Sicfarma request falhou para farmácia %s: HTTP %s",
            cod_farmacia,
            response.status_code,
        )
        return None

    try:
        dados = response.json()
    except Exception as e:
        logger.warning("Sicfarma resposta inválida (JSON) para farmácia %s: %s", cod_farmacia, e)
        return None

    if isinstance(dados, list):
        if not dados:
            return None
        dados = dados[0]

    codigo_classificacao = dados.get("classificacaoFarmacia")
    if codigo_classificacao is None:
        return None

    try:
        codigo_int = int(codigo_classificacao)
    except (TypeError, ValueError):
        logger.warning(
            "Sicfarma classificacaoFarmacia inesperado para farmácia %s: %r",
            cod_farmacia,
            codigo_classificacao,
        )
        return None

    return _SICFARMA_CLASSIFICATION_MAP.get(codigo_int)


def buscar_versao_por_codigo(cod_farmacia: str) -> str | None:

    if not SICFARMA_URL:
        return None

    url = f"{SICFARMA_URL}/versoes"

    try:
        response = _get_with_retry(
            url,
            params={"id": cod_farmacia},
            auth=(SICFARMA_USERNAME, SICFARMA_PASSWORD),
            timeout=10,
        )
    except Exception as e:
        logger.warning(
            "Sicfarma /versoes request falhou para farmácia %s: %s", cod_farmacia, e
        )
        return None

    if response.status_code != 200:
        logger.warning(
            "Sicfarma /versoes request falhou para farmácia %s: HTTP %s",
            cod_farmacia,
            response.status_code,
        )
        return None

    try:
        dados = response.json()
    except Exception as e:
        logger.warning("Sicfarma /versoes resposta inválida (JSON) para farmácia %s: %s", cod_farmacia, e)
        return None

    if not isinstance(dados, list):
        dados = [dados]

    for item in dados:
        if isinstance(item, dict) and item.get("codSistema") == COD_SISTEMA_COLETOR:
            versao = item.get("numVersao")
            return str(versao) if versao is not None else None

    return None


def buscar_versoes_farmacias(codigos: list[str]) -> dict[str, str | None]:

    if not codigos:
        return {}

    t_inicio = time.perf_counter()
    logger.info("⏳ Consultando versão coletor Sicfarma para %d farmácias...", len(codigos))

    resultado: dict[str, str | None] = {}

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(buscar_versao_por_codigo, cod): cod for cod in codigos}
        for future in as_completed(futures):
            cod = futures[future]
            try:
                resultado[cod] = future.result()
            except Exception as e:
                logger.warning("Sicfarma /versoes falhou para farmácia %s: %s", cod, e)
                resultado[cod] = None

    logger.info("✅ Sicfarma /versoes — %d farmácias consultadas em %.2fs", len(resultado), time.perf_counter() - t_inicio)
    return resultado


def buscar_classificacao_farmacias(codigos: list[str]) -> dict[str, str | None]:

    if not codigos:
        return {}

    t_inicio = time.perf_counter()
    logger.info("⏳ Consultando classificação Sicfarma para %d farmácias...", len(codigos))

    resultado: dict[str, str | None] = {}

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(buscar_classificacao_por_codigo, cod): cod for cod in codigos}
        for future in as_completed(futures):
            cod = futures[future]
            try:
                resultado[cod] = future.result()
            except Exception as e:
                logger.warning("Sicfarma classificação falhou para farmácia %s: %s", cod, e)
                resultado[cod] = None

    logger.info("✅ Sicfarma — %d farmácias consultadas em %.2fs", len(resultado), time.perf_counter() - t_inicio)
    return resultado

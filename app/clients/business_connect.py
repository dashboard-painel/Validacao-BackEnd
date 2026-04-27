import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

_BC_BASE_URL = "https://business-connect.triercloud.com.br/v1"


def _formatar_data_upload(raw: str) -> str:

    raw = (raw or "").strip()
    if not raw:
        return raw
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(raw[:19], fmt).strftime("%d/%m/%Y %H:%M:%S")
        except ValueError:
            continue
    try:
        return datetime.strptime(raw[:10], "%Y-%m-%d").strftime("%d/%m/%Y 00:00:00")
    except ValueError:
        return raw


def get_bearer_token() -> str:
    """Autentica no Business Connect via form POST e retorna o Bearer token.

    Usa as variáveis de ambiente BC_USERNAME (campo 'code') e BC_PASSWORD.

    Returns:
        str: Bearer token para usar no header Authorization

    Raises:
        Exception: Se a autenticação falhar (status HTTP != 200)
    """
    username = os.getenv("BC_USERNAME", "")
    password = os.getenv("BC_PASSWORD", "")

    response = requests.post(
        f"{_BC_BASE_URL}/auth",
        data={"code": username, "password": password},
        timeout=30,
    )

    if response.status_code != 200:
        raise Exception(
            f"Business Connect auth falhou: HTTP {response.status_code} — {response.text[:300]}"
        )

    data = response.json()

    token = data.get("access") or data.get("access_token") or data.get("token") or data.get("accessToken")
    if not token:
        raise Exception(
            f"Business Connect auth: token não encontrado na resposta. Chaves disponíveis: {list(data.keys())}"
        )
    return token


def get_status_farmacia(cod_farmacia: str, token: str) -> str:

    url = f"{_BC_BASE_URL}/migration/pharmacy/{cod_farmacia}/status"
    try:
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
    except requests.RequestException as e:
        logger.warning("Business Connect request falhou para farmácia %s: %s", cod_farmacia, e)
        return "Sem pendências"

    if response.status_code == 404:
        return "Sem pendências"

    if response.status_code != 200:
        logger.warning(
            "Business Connect status inesperado para farmácia %s: HTTP %s",
            cod_farmacia,
            response.status_code,
        )
        return "Sem pendências"

    try:
        registros = response.json()
    except Exception as e:
        logger.warning("Business Connect resposta inválida para farmácia %s: %s", cod_farmacia, e)
        return "OK, sem registro"

    for registro in registros:
        if isinstance(registro, dict) and registro.get("table_name") == "cadcvend":
            data_upload = registro.get("data_upload_datalake", "")
            return f"Pendente de envio no dia {_formatar_data_upload(data_upload)}"

    return "Sem pendências"


def buscar_status_farmacias(codigos: list[str]) -> dict[str, str]:

    if not codigos:
        return {}

    logger.info("⏳ Autenticando no Business Connect...")
    t_auth = time.perf_counter()
    token = get_bearer_token()
    logger.info("✅ Business Connect autenticado em %.2fs — consultando %d farmácias...", time.perf_counter() - t_auth, len(codigos))

    resultado: dict[str, str] = {}
    t_parallel = time.perf_counter()
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(get_status_farmacia, cod, token): cod for cod in codigos}
        for future in as_completed(futures):
            cod = futures[future]
            try:
                resultado[cod] = future.result()
            except Exception as e:
                logger.warning("Erro ao buscar status farmácia %s: %s", cod, e)
                resultado[cod] = "Sem pendências"

    logger.info("✅ Business Connect — %d farmácias consultadas em %.2fs", len(resultado), time.perf_counter() - t_parallel)
    return resultado

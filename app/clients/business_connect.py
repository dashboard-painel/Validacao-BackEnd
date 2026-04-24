"""Módulo de integração com a API Business Connect para status de migração."""
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

logger = logging.getLogger(__name__)

_BC_BASE_URL = "https://business-connect.triercloud.com.br/v1"


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
    # A API pode retornar o token sob chaves diferentes — tentamos as mais comuns
    token = data.get("access") or data.get("access_token") or data.get("token") or data.get("accessToken")
    if not token:
        raise Exception(
            f"Business Connect auth: token não encontrado na resposta. Chaves disponíveis: {list(data.keys())}"
        )
    return token


def get_status_farmacia(cod_farmacia: str, token: str) -> str:
    """Consulta o status de migração de uma farmácia no Business Connect.

    Procura na resposta qualquer registro onde table_name == "cadcvend".
    - Se encontrado: retorna "Pendente de envio no dia {data_upload_datalake}"
    - Se não encontrado ou farmácia não cadastrada (404): retorna "OK, sem registro"
    - Se request falhar por outro motivo: loga warning e retorna "OK, sem registro"

    Args:
        cod_farmacia: Código da farmácia a consultar
        token: Bearer token obtido via get_bearer_token()

    Returns:
        str: Texto descritivo do status coletor_novo
    """
    url = f"{_BC_BASE_URL}/migration/pharmacy/{cod_farmacia}/status"
    try:
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
    except requests.RequestException as e:
        logger.warning("Business Connect request falhou para farmácia %s: %s", cod_farmacia, e)
        return "OK, sem registro"

    if response.status_code == 404:
        return "OK, sem registro"

    if response.status_code != 200:
        logger.warning(
            "Business Connect status inesperado para farmácia %s: HTTP %s",
            cod_farmacia,
            response.status_code,
        )
        return "OK, sem registro"

    try:
        registros = response.json()
    except Exception as e:
        logger.warning("Business Connect resposta inválida para farmácia %s: %s", cod_farmacia, e)
        return "OK, sem registro"

    # Procura registro onde table_name == "cadcvend"
    for registro in registros:
        if isinstance(registro, dict) and registro.get("table_name") == "cadcvend":
            data_upload = registro.get("data_upload_datalake", "")
            return f"Pendente de envio no dia {data_upload}"

    return "OK, sem registro"


def buscar_status_farmacias(codigos: list[str]) -> dict[str, str]:
    """Obtém o status de migração para uma lista de farmácias em paralelo.

    Obtém o Bearer token UMA única vez e reutiliza para todas as consultas.
    Usa ThreadPoolExecutor para consultar múltiplas farmácias em paralelo.
    Se a lista de códigos estiver vazia, retorna dict vazio sem fazer requests.

    Args:
        codigos: Lista de códigos de farmácia a consultar

    Returns:
        dict[str, str]: Mapeamento {cod_farmacia: coletor_novo}

    Raises:
        Exception: Se a autenticação falhar (propagado para o caller decidir o fallback)
    """
    if not codigos:
        return {}

    logger.info("⏳ Autenticando no Business Connect...")
    t_auth = time.perf_counter()
    token = get_bearer_token()
    logger.info("✅ Business Connect autenticado em %.2fs — consultando %d farmácias...", time.perf_counter() - t_auth, len(codigos))

    resultado: dict[str, str] = {}
    t_parallel = time.perf_counter()
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(get_status_farmacia, cod, token): cod for cod in codigos}
        for future in as_completed(futures):
            cod = futures[future]
            try:
                resultado[cod] = future.result()
            except Exception as e:
                logger.warning("Erro ao buscar status farmácia %s: %s", cod, e)
                resultado[cod] = "OK, sem registro"

    logger.info("✅ Business Connect — %d farmácias consultadas em %.2fs", len(resultado), time.perf_counter() - t_parallel)
    return resultado

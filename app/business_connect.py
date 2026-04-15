"""Módulo de integração com a API Business Connect para status de migração."""
import logging
import os

import requests

logger = logging.getLogger(__name__)

_BC_BASE_URL = "https://business-connect.triercloud.com.br/v1"


def get_bearer_token() -> str:
    """Autentica no Business Connect via HTTP Basic Auth e retorna o Bearer token.

    Usa as variáveis de ambiente BC_USERNAME e BC_PASSWORD.

    Returns:
        str: Bearer token para usar no header Authorization

    Raises:
        Exception: Se a autenticação falhar (status HTTP != 200)
    """
    username = os.getenv("BC_USERNAME", "")
    password = os.getenv("BC_PASSWORD", "")

    response = requests.post(
        f"{_BC_BASE_URL}/auth",
        auth=(username, password),
        timeout=10,
    )

    if response.status_code != 200:
        raise Exception(
            f"Business Connect auth falhou: HTTP {response.status_code} — {response.text[:300]}"
        )

    data = response.json()
    # A API pode retornar o token sob chaves diferentes — tentamos as mais comuns
    token = data.get("access_token") or data.get("token") or data.get("accessToken")
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
            timeout=10,
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
    """Obtém o status de migração para uma lista de farmácias.

    Obtém o Bearer token UMA única vez e reutiliza para todas as consultas.
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

    token = get_bearer_token()

    resultado: dict[str, str] = {}
    for cod in codigos:
        resultado[cod] = get_status_farmacia(cod, token)

    return resultado

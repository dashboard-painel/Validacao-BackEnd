"""Módulo de queries SQL para execução no Redshift."""
import logging
import time

from app.database import get_connection

logger = logging.getLogger(__name__)

QUERY_GOLD_VENDAS = """
SELECT
    cod_farmacia,
    nome_farmacia,
    cnpj,
    ultima_venda,
    ultima_hora_venda
FROM (
    SELECT
        codigo AS cod_farmacia,
        nom_fantasia AS nome_farmacia,
        num_cnpj AS cnpj,
        dat_emissao AS ultima_venda,
        dat_hora_emissao AS ultima_hora_venda,
        ROW_NUMBER() OVER (
            PARTITION BY codigo
            ORDER BY dat_emissao DESC, dat_hora_emissao DESC
        ) AS rn
    FROM
        associacao.vendas
    WHERE
        associacao = %s
) sub
WHERE rn = 1
ORDER BY ultima_venda DESC, ultima_hora_venda DESC;
"""

QUERY_SILVER_STGN_DEDUP = """
SELECT
    cod_farmacia,
    ultima_venda,
    ultima_hora_venda
FROM (
    SELECT
        codigo_farmacia AS cod_farmacia,
        dat_emissao AS ultima_venda,
        hor_emissao AS ultima_hora_venda,
        ROW_NUMBER() OVER (
            PARTITION BY codigo_farmacia
            ORDER BY dat_emissao DESC, hor_emissao DESC
        ) AS rn
    FROM
        silver.cadcvend_staging_dedup
    WHERE
        associacao = %s
) sub
WHERE rn = 1
ORDER BY ultima_venda DESC, ultima_hora_venda DESC;
"""

def execute_gold_vendas(associacao: str) -> list[dict]:
    """Executa a query na tabela associacao.vendas (GoldVendas) no Redshift.

    Retorna a última venda registrada por farmácia (cod_farmacia), sem filtro de data.

    Args:
        associacao: Código da associação para filtrar

    Returns:
        Lista de dicionários com chaves: cod_farmacia, nome_farmacia, cnpj,
        ultima_venda, ultima_hora_venda
    """
    logger.info("⏳ Aguardando resposta Redshift [GoldVendas] — associacao=%s...", associacao)
    t0 = time.perf_counter()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(QUERY_GOLD_VENDAS, (associacao,))
        column_names = [desc[0] for desc in cursor.description]
        rows = [dict(zip(column_names, row)) for row in cursor.fetchall()]
    elapsed = time.perf_counter() - t0
    logger.info("✅ Redshift [GoldVendas] respondeu em %.2fs — %d registros retornados", elapsed, len(rows))
    return rows


def execute_cadfilia_por_codigos(codigos: list[str]) -> dict[str, dict]:
    """Busca nome e CNPJ de farmácias em duas fontes e faz merge priorizando dados preenchidos.

    Consulta em paralelo:
    - silver.cadfilia_staging_dedup
    - associacao.dimensao_cadastro_lojas

    Para cada farmácia, usa o valor não-nulo disponível (cadfilia tem prioridade;
    dimensao_cadastro_lojas preenche os gaps).

    Args:
        codigos: Lista de cod_farmacia a consultar

    Returns:
        Dict {cod_farmacia: {"nome_farmacia": ..., "cnpj": ...}}
    """
    if not codigos:
        return {}

    placeholders = ",".join(["%s"] * len(codigos))

    query_cadfilia = f"""
        SELECT cod_farmacia, MAX(nom_fantasia) AS nome_farmacia, MAX(num_cnpj) AS cnpj
        FROM silver.cadfilia_staging_dedup
        WHERE cod_farmacia IN ({placeholders})
        GROUP BY cod_farmacia
    """

    query_dimensao = f"""
        SELECT cod_farmacia, MAX(nom_fantasia) AS nome_farmacia, MAX(num_cnpj) AS cnpj
        FROM associacao.dimensao_cadastro_lojas
        WHERE cod_farmacia IN ({placeholders})
        GROUP BY cod_farmacia
    """

    logger.info("⏳ Aguardando resposta Redshift [cadfilia + dimensao_cadastro_lojas] — %d códigos...", len(codigos))
    t0 = time.perf_counter()

    with get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute(query_cadfilia, list(codigos))
        cadfilia = {str(row[0]).strip(): {"nome_farmacia": row[1], "cnpj": row[2]} for row in cursor.fetchall()}

        cursor.execute(query_dimensao, list(codigos))
        dimensao = {str(row[0]).strip(): {"nome_farmacia": row[1], "cnpj": row[2]} for row in cursor.fetchall()}

    # Merge: cadfilia como base, dimensao preenche nulls
    todos_codigos = set(cadfilia.keys()) | set(dimensao.keys())
    resultado = {}
    for cod in todos_codigos:
        c = cadfilia.get(cod, {})
        d = dimensao.get(cod, {})
        resultado[cod] = {
            "nome_farmacia": c.get("nome_farmacia") or d.get("nome_farmacia"),
            "cnpj": c.get("cnpj") or d.get("cnpj"),
        }

    elapsed = time.perf_counter() - t0
    logger.info("✅ Redshift [cadfilia + dimensao] respondeu em %.2fs — %d registros mesclados", elapsed, len(resultado))
    return resultado


def execute_silver_stgn_dedup(associacao: str) -> list[dict]:
    """Executa a query na tabela silver.cadcvend_staging_dedup (SilverSTGN_Dedup) no Redshift.

    Retorna a última venda registrada por farmácia (cod_farmacia), sem filtro de data.

    Args:
        associacao: Código da associação para filtrar

    Returns:
        Lista de dicionários com chaves: cod_farmacia, ultima_venda,
        ultima_hora_venda
    """
    logger.info("⏳ Aguardando resposta Redshift [SilverSTGN_Dedup] — associacao=%s...", associacao)
    t0 = time.perf_counter()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(QUERY_SILVER_STGN_DEDUP, (associacao,))
        column_names = [desc[0] for desc in cursor.description]
        rows = [dict(zip(column_names, row)) for row in cursor.fetchall()]
    elapsed = time.perf_counter() - t0
    logger.info("✅ Redshift [SilverSTGN_Dedup] respondeu em %.2fs — %d registros retornados", elapsed, len(rows))
    return rows

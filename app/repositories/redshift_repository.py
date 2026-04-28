import logging
import time

from app.db.redshift import get_connection

logger = logging.getLogger(__name__)

QUERY_GOLD_VENDAS = """
SELECT
    cod_farmacia,
    nome_farmacia,
    cnpj,
    sit_contrato,
    codigo_rede,
    ultima_venda,
    ultima_hora_venda
FROM (
    SELECT
        d.cod_farmacia,
        d.nom_farmacia AS nome_farmacia,
        d.num_cnpj AS cnpj,
        d.sit_contrato,
        d.codigo_rede,
        v.dat_emissao AS ultima_venda,
        v.dat_hora_emissao AS ultima_hora_venda,
        ROW_NUMBER() OVER (
            PARTITION BY d.cod_farmacia
            ORDER BY v.dat_emissao DESC, v.dat_hora_emissao DESC
        ) AS rn
    FROM associacao.dimensao_cadastro_lojas d
    LEFT JOIN associacao.vendas v
        ON v.codigo = d.cod_farmacia
        AND v.associacao = %s
    WHERE d.codigo_rede = %s
    AND d.sistema = 'TRIER' -- filtro
) sub
WHERE rn = 1
ORDER BY ultima_venda DESC, ultima_hora_venda DESC;
"""

# QUERY_SILVER_STGN_DEDUP = """
# SELECT
#     cod_farmacia,
#     ultima_venda,
#     ultima_hora_venda
# FROM (
#     SELECT
#         codigo_farmacia AS cod_farmacia,
#         dat_emissao AS ultima_venda,
#         hor_emissao AS ultima_hora_venda,
#         ROW_NUMBER() OVER (
#             PARTITION BY codigo_farmacia
#             ORDER BY dat_emissao DESC, hor_emissao DESC
#         ) AS rn
#     FROM
#         silver.cadcvend_staging_dedup
#     WHERE
#         associacao = %s
# ) sub
# WHERE rn = 1
# ORDER BY ultima_venda DESC, ultima_hora_venda DESC;
# """

QUERY_VENDAS_PARCEIROS = """
SELECT
    dmj.cod_farmacia,
    dmj.nom_farmacia AS nome_farmacia,
    dmj.sit_contrato,
    dmj.codigo_rede AS associacao,
    vp.farmacia,
    vp.associacao AS associacao_parceiro,
    MAX(vp.dat_hora_emissao) AS ultima_venda_parceiros
FROM associacao.vendas_parceiros vp
JOIN associacao.dimensao_cadastro_lojas dmj
    ON dmj.num_cnpj = vp.num_cnpj
GROUP BY
    dmj.cod_farmacia,
    dmj.nom_farmacia,
    dmj.sit_contrato,
    dmj.codigo_rede,
    vp.farmacia,
    vp.associacao
ORDER BY ultima_venda_parceiros DESC;
"""

def execute_gold_vendas(associacao: str) -> list[dict]:
    logger.info("⏳ Aguardando resposta Redshift [associacao.vendas] — associacao=%s...", associacao)
    t0 = time.perf_counter()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(QUERY_GOLD_VENDAS, (associacao,associacao))
        column_names = [desc[0] for desc in cursor.description]
        rows = [dict(zip(column_names, row)) for row in cursor.fetchall()]
    elapsed = time.perf_counter() - t0
    logger.info("✅ Redshift [associacao.vendas] respondeu em %.2fs — %d registros retornados", elapsed, len(rows))
    return rows


def execute_dimensao_por_codigos(codigos: list[str]) -> dict[str, dict]:
    if not codigos:
        return {}

    placeholders = ",".join(["%s"] * len(codigos))

    query_dimensao = f"""
        SELECT
            cod_farmacia,
            MAX(nom_farmacia) AS nome_farmacia,
            MAX(num_cnpj) AS cnpj,
            MAX(sit_contrato) AS sit_contrato,
            MAX(codigo_rede) AS codigo_rede
        FROM associacao.dimensao_cadastro_lojas
        WHERE cod_farmacia IN ({placeholders})
        GROUP BY cod_farmacia
    """

    logger.info(
        "⏳ Aguardando resposta Redshift [dimensao_cadastro_lojas] — %d códigos...",
        len(codigos),
    )
    t0 = time.perf_counter()

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query_dimensao, list(codigos))
        resultado = {
            str(row[0]).strip(): {
                "nome_farmacia": row[1],
                "cnpj": row[2],
                "sit_contrato": row[3],
                "codigo_rede": row[4],
            }
            for row in cursor.fetchall()
        }

    elapsed = time.perf_counter() - t0
    logger.info(
        "✅ Redshift [dimensao_cadastro_lojas] respondeu em %.2fs — %d registros",
        elapsed,
        len(resultado),
    )
    return resultado


# def execute_silver_stgn_dedup(associacao: str) -> list[dict]:
#     logger.info("⏳ Aguardando resposta Redshift [silver.cadcvend_staging_dedup] — associacao=%s...", associacao)
#     t0 = time.perf_counter()
#     with get_connection() as conn:
#         cursor = conn.cursor()
#         cursor.execute(QUERY_SILVER_STGN_DEDUP, (associacao,))
#         column_names = [desc[0] for desc in cursor.description]
#         rows = [dict(zip(column_names, row)) for row in cursor.fetchall()]
#     elapsed = time.perf_counter() - t0
#     logger.info("✅ Redshift [silver.cadcvend_staging_dedup] respondeu em %.2fs — %d registros retornados", elapsed, len(rows))
#     return rows


def execute_vendas_parceiros() -> list[dict]:
    logger.info("⏳ Aguardando resposta Redshift [associacao.vendas_parceiros]...")
    t0 = time.perf_counter()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(QUERY_VENDAS_PARCEIROS)
        column_names = [desc[0] for desc in cursor.description]
        rows = [dict(zip(column_names, row)) for row in cursor.fetchall()]
    elapsed = time.perf_counter() - t0
    logger.info("✅ Redshift [associacao.vendas_parceiros] respondeu em %.2fs — %d registros retornados", elapsed, len(rows))
    return rows

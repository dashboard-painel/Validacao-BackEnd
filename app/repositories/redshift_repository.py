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
    sit_contrato,
    codigo_rede,
    ultima_venda,
    ultima_hora_venda
FROM (
    SELECT
        v.codigo AS cod_farmacia,
        d.nom_farmacia AS nome_farmacia,
        d.num_cnpj AS cnpj,
        d.sit_contrato AS sit_contrato,
        d.codigo_rede AS codigo_rede,
        v.dat_emissao AS ultima_venda,
        v.dat_hora_emissao AS ultima_hora_venda,
        ROW_NUMBER() OVER (
            PARTITION BY v.codigo
            ORDER BY v.dat_emissao DESC, v.dat_hora_emissao DESC
        ) AS rn
    FROM
        associacao.vendas v
    LEFT JOIN
        associacao.dimensao_cadastro_lojas d ON d.cod_farmacia = v.codigo
    WHERE
        v.associacao = %s
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
    Dados cadastrais (nome_farmacia, cnpj, sit_contrato, codigo_rede) são obtidos via
    LEFT JOIN com associacao.dimensao_cadastro_lojas.

    Args:
        associacao: Código da associação para filtrar

    Returns:
        Lista de dicionários com chaves: cod_farmacia, nome_farmacia, cnpj,
        sit_contrato, codigo_rede, ultima_venda, ultima_hora_venda
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
    """Busca dados cadastrais de farmácias em associacao.dimensao_cadastro_lojas.

    Args:
        codigos: Lista de cod_farmacia a consultar

    Returns:
        Dict {cod_farmacia: {"nome_farmacia": ..., "cnpj": ..., "sit_contrato": ..., "codigo_rede": ...}}
    """
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
        "✅ Redshift [dimensao_cadastro_lojas] respondeu em %.2fs — %d registros retornados",
        elapsed,
        len(resultado),
    )
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

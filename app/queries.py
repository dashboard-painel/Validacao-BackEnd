"""Módulo de queries SQL para execução no Redshift."""
from app.database import get_connection

QUERY_1 = """
SELECT
    cod_farmacia,
    nome_farmacia,
    ultima_venda,
    ultima_hora_venda
FROM (
    SELECT
        codigo AS cod_farmacia,
        nom_fantasia AS nome_farmacia,
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
        AND dat_emissao > %s
) sub
WHERE rn = 1
ORDER BY ultima_venda DESC, ultima_hora_venda DESC;
"""

QUERY_2 = """
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
        AND dat_emissao > %s
) sub
WHERE rn = 1
ORDER BY ultima_venda DESC, ultima_hora_venda DESC;
"""


def execute_query_1(associacao: str, dat_emissao: str) -> list[dict]:
    """Executa a Query 1 na tabela associacao.vendas no Redshift.

    Retorna a última venda por farmácia (cod_farmacia) após a data informada,
    incluindo o nome da farmácia e hora da última venda.

    Args:
        associacao: Código da associação para filtrar
        dat_emissao: Data de emissão mínima no formato YYYY-MM-DD

    Returns:
        Lista de dicionários com chaves: cod_farmacia, nome_farmacia,
        ultima_venda, ultima_hora_venda
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(QUERY_1, (associacao, dat_emissao))
        column_names = [desc[0] for desc in cursor.description]
        return [dict(zip(column_names, row)) for row in cursor.fetchall()]


def execute_query_2(associacao: str, dat_emissao: str) -> list[dict]:
    """Executa a Query 2 na tabela silver.cadcvend_staging_dedup no Redshift.

    Retorna a última venda por farmácia (cod_farmacia) após a data informada.

    Args:
        associacao: Código da associação para filtrar
        dat_emissao: Data de emissão mínima no formato YYYY-MM-DD

    Returns:
        Lista de dicionários com chaves: cod_farmacia, ultima_venda,
        ultima_hora_venda
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(QUERY_2, (associacao, dat_emissao))
        column_names = [desc[0] for desc in cursor.description]
        return [dict(zip(column_names, row)) for row in cursor.fetchall()]

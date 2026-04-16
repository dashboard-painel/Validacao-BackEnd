"""Módulo de queries SQL para execução no Redshift."""
from app.database import get_connection

QUERY_GOLD_VENDAS = """
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
        AND dat_emissao > %s
) sub
WHERE rn = 1
ORDER BY ultima_venda DESC, ultima_hora_venda DESC;
"""


QUERY_NOMES_FARMACIAS = """
SELECT DISTINCT
    codigo AS cod_farmacia,
    nom_fantasia AS nome_farmacia
FROM
    associacao.vendas
WHERE
    associacao = %s;
"""


def execute_gold_vendas(associacao: str, dat_emissao: str) -> list[dict]:
    """Executa a query na tabela associacao.vendas (GoldVendas) no Redshift.

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
        cursor.execute(QUERY_GOLD_VENDAS, (associacao, dat_emissao))
        column_names = [desc[0] for desc in cursor.description]
        return [dict(zip(column_names, row)) for row in cursor.fetchall()]


def buscar_nomes_farmacias(associacao: str) -> dict[str, str]:
    """Busca nomes de todas as farmácias da associação em associacao.vendas.

    Chamada uma única vez por comparação para montar o dicionário de nomes,
    evitando queries adicionais por farmácia individual.

    Args:
        associacao: Código da associação para filtrar

    Returns:
        Dict {cod_farmacia: nome_farmacia}
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(QUERY_NOMES_FARMACIAS, (associacao,))
        return {str(row[0]): row[1] for row in cursor.fetchall()}


def execute_silver_stgn_dedup(associacao: str, dat_emissao: str) -> list[dict]:
    """Executa a query na tabela silver.cadcvend_staging_dedup (SilverSTGN_Dedup) no Redshift.

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
        cursor.execute(QUERY_SILVER_STGN_DEDUP, (associacao, dat_emissao))
        column_names = [desc[0] for desc in cursor.description]
        return [dict(zip(column_names, row)) for row in cursor.fetchall()]

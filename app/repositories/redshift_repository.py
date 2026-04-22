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
    """Busca dados cadastrais de farmácias priorizando associacao.dimensao_cadastro_lojas
    e usando silver.cadfilia_staging_dedup apenas para preencher campos nulos.

    Fluxo:
    - dimensao_cadastro_lojas é a fonte principal (inclui sit_contrato e codigo_rede)
    - cadfilia_staging_dedup preenche apenas nome_farmacia e cnpj quando nulos na dimensao

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

    query_cadfilia = f"""
        SELECT cod_farmacia, MAX(nom_fantasia) AS nome_farmacia, MAX(num_cnpj) AS cnpj
        FROM silver.cadfilia_staging_dedup
        WHERE cod_farmacia IN ({placeholders})
        GROUP BY cod_farmacia
    """

    logger.info(
        "⏳ Aguardando resposta Redshift [dimensao_cadastro_lojas + cadfilia gap-fill] — %d códigos...",
        len(codigos),
    )
    t0 = time.perf_counter()

    with get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute(query_dimensao, list(codigos))
        dimensao = {
            str(row[0]).strip(): {
                "nome_farmacia": row[1],
                "cnpj": row[2],
                "sit_contrato": row[3],
                "codigo_rede": row[4],
            }
            for row in cursor.fetchall()
        }

        # Consulta cadfilia apenas se houver gaps de nome ou CNPJ na dimensao
        codigos_com_gap = [
            cod for cod in codigos
            if not dimensao.get(str(cod).strip(), {}).get("nome_farmacia")
            or not dimensao.get(str(cod).strip(), {}).get("cnpj")
        ]
        cadfilia: dict[str, dict] = {}
        if codigos_com_gap:
            placeholders_gap = ",".join(["%s"] * len(codigos_com_gap))
            query_cadfilia_gap = query_cadfilia.replace(f"IN ({placeholders})", f"IN ({placeholders_gap})")
            cursor.execute(query_cadfilia_gap, codigos_com_gap)
            cadfilia = {str(row[0]).strip(): {"nome_farmacia": row[1], "cnpj": row[2]} for row in cursor.fetchall()}

    # Merge: dimensao como base, cadfilia preenche nome/cnpj nulos
    todos_codigos = set(str(c).strip() for c in codigos)
    resultado = {}
    for cod in todos_codigos:
        d = dimensao.get(cod, {})
        c = cadfilia.get(cod, {})
        resultado[cod] = {
            "nome_farmacia": d.get("nome_farmacia") or c.get("nome_farmacia"),
            "cnpj": d.get("cnpj") or c.get("cnpj"),
            "sit_contrato": d.get("sit_contrato"),
            "codigo_rede": d.get("codigo_rede"),
        }

    elapsed = time.perf_counter() - t0
    logger.info(
        "✅ Redshift [dimensao + cadfilia gap-fill] respondeu em %.2fs — %d registros (gap-fill em %d)",
        elapsed,
        len(resultado),
        len(codigos_com_gap),
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

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
    ultima_hora_venda,
    num_versao
FROM (
    SELECT
        d.cod_farmacia,
        d.nom_farmacia AS nome_farmacia,
        d.num_cnpj AS cnpj,
        d.sit_contrato,
        d.codigo_rede,
        v.dat_emissao AS ultima_venda,
        v.dat_hora_emissao AS ultima_hora_venda,
        vc.num_versao,
        ROW_NUMBER() OVER (
            PARTITION BY d.cod_farmacia
            ORDER BY v.dat_emissao DESC, v.dat_hora_emissao DESC
        ) AS rn
    FROM associacao.dimensao_cadastro_lojas d
    LEFT JOIN associacao.vendas v
        ON v.codigo = d.cod_farmacia
        AND v.associacao = %s
    LEFT JOIN associacao.versoes_coletor vc
        ON vc.cod_farmacia = d.cod_farmacia
    WHERE d.codigo_rede = %s
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
    """Executa a query na tabela associacao.vendas (GoldVendas) no Redshift.

    Retorna a última venda registrada por farmácia (cod_farmacia), sem filtro de data.
    Dados cadastrais (nome_farmacia, cnpj, sit_contrato, codigo_rede) são obtidos via
    LEFT JOIN com associacao.dimensao_cadastro_lojas.

    Args:
        associacao: Código da associação para filtrar

    Returns:
        Lista de dicionários com chaves: cod_farmacia, nome_farmacia, cnpj,
        sit_contrato, codigo_rede, ultima_venda, ultima_hora_venda, num_versao
    """
    logger.info("⏳ Aguardando resposta Redshift [GoldVendas] — associacao=%s...", associacao)
    t0 = time.perf_counter()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(QUERY_GOLD_VENDAS, (associacao,associacao))
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
        SELECT cod_farmacia,
               MAX(COALESCE(nom_fantasia, raz_social)) AS nome_farmacia,
               MAX(num_cnpj) AS cnpj,
               MAX(flg_ativo) AS flg_ativo
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

        # Consulta cadfilia se houver gaps de nome, CNPJ ou sit_contrato na dimensao
        codigos_com_gap = [
            cod for cod in codigos
            if not dimensao.get(str(cod).strip(), {}).get("nome_farmacia")
            or not dimensao.get(str(cod).strip(), {}).get("cnpj")
            or not dimensao.get(str(cod).strip(), {}).get("sit_contrato")
        ]
        cadfilia: dict[str, dict] = {}
        if codigos_com_gap:
            placeholders_gap = ",".join(["%s"] * len(codigos_com_gap))
            query_cadfilia_gap = query_cadfilia.replace(f"IN ({placeholders})", f"IN ({placeholders_gap})")
            cursor.execute(query_cadfilia_gap, codigos_com_gap)
            cadfilia = {
                str(row[0]).strip(): {"nome_farmacia": row[1], "cnpj": row[2], "flg_ativo": row[3]}
                for row in cursor.fetchall()
            }

    # Merge: dimensao como base, cadfilia preenche nome/cnpj nulos
    todos_codigos = set(str(c).strip() for c in codigos)
    resultado = {}
    for cod in todos_codigos:
        d = dimensao.get(cod, {})
        c = cadfilia.get(cod, {})
        sit_contrato = d.get("sit_contrato")
        if not sit_contrato and c.get("flg_ativo"):
            sit_contrato = "ATIVO" if str(c["flg_ativo"]).strip().upper() == "A" else None
        resultado[cod] = {
            "nome_farmacia": d.get("nome_farmacia") or c.get("nome_farmacia"),
            "cnpj": d.get("cnpj") or c.get("cnpj"),
            "sit_contrato": sit_contrato,
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


def execute_vendas_parceiros() -> list[dict]:
    """Executa a query de vendas_parceiros JOIN dimensao_cadastro_lojas no Redshift.

    Retorna dados cadastrais da farmácia + última venda em vendas_parceiros,
    cruzando pelo CNPJ entre as tabelas. Sem filtro — traz todas as redes.

    Returns:
        Lista de dicionários com chaves: cod_farmacia, nome_farmacia, sit_contrato,
        associacao, farmacia, associacao_parceiro, ultima_venda_parceiros
    """
    logger.info("⏳ Aguardando resposta Redshift [VendasParceiros]...")
    t0 = time.perf_counter()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(QUERY_VENDAS_PARCEIROS)
        column_names = [desc[0] for desc in cursor.description]
        rows = [dict(zip(column_names, row)) for row in cursor.fetchall()]
    elapsed = time.perf_counter() - t0
    logger.info("✅ Redshift [VendasParceiros] respondeu em %.2fs — %d registros retornados", elapsed, len(rows))
    return rows

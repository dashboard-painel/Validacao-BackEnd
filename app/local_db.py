"""Módulo de persistência local no PostgreSQL para resultados de comparação."""
import os
import re
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()


def _sanitize_cnpj(cnpj: Optional[str]) -> Optional[str]:
    """Remove pontuação do CNPJ, retornando apenas os 14 dígitos."""
    if not cnpj:
        return cnpj
    return re.sub(r"[.\-/]", "", cnpj)


def get_local_connection():
    """Retorna conexão com o PostgreSQL local."""
    url = os.getenv("LOCAL_DB_URL", "")
    url = url.replace("jdbc:", "", 1)
    user = os.getenv("LOCAL_DB_USER")
    password = os.getenv("LOCAL_DB_PASS")
    if user and password and "@" not in url:
        url = url.replace("postgresql://", f"postgresql://{user}:{password}@", 1)
    return psycopg2.connect(url)


def init_local_db():
    """Cria as tabelas necessárias no PostgreSQL local se não existirem."""
    with get_local_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS comparacoes (
                    id SERIAL PRIMARY KEY,
                    associacao VARCHAR(100) NOT NULL,
                    executado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    total_gold_vendas INTEGER NOT NULL,
                    total_silver_stgn_dedup INTEGER NOT NULL,
                    total_divergencias INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS resultados_gold_vendas (
                    id SERIAL PRIMARY KEY,
                    associacao VARCHAR(100) NOT NULL,
                    comparacao_id INTEGER REFERENCES comparacoes(id),
                    cod_farmacia VARCHAR(50) NOT NULL,
                    nome_farmacia VARCHAR(255),
                    cnpj VARCHAR(30),
                    ultima_venda DATE,
                    ultima_hora_venda TIMESTAMP,
                    UNIQUE (associacao, cod_farmacia)
                );

                CREATE TABLE IF NOT EXISTS resultados_silver_stgn_dedup (
                    id SERIAL PRIMARY KEY,
                    associacao VARCHAR(100) NOT NULL,
                    comparacao_id INTEGER REFERENCES comparacoes(id),
                    cod_farmacia VARCHAR(50) NOT NULL,
                    ultima_venda DATE,
                    ultima_hora_venda TIME,
                    UNIQUE (associacao, cod_farmacia)
                );

                CREATE TABLE IF NOT EXISTS resultados_consolidados (
                    id SERIAL PRIMARY KEY,
                    associacao VARCHAR(100) NOT NULL,
                    comparacao_id INTEGER REFERENCES comparacoes(id),
                    cod_farmacia VARCHAR(50) NOT NULL,
                    nome_farmacia VARCHAR(255),
                    cnpj VARCHAR(30),
                    ultima_venda_GoldVendas DATE,
                    ultima_hora_venda_GoldVendas TIMESTAMP,
                    ultima_venda_SilverSTGN_Dedup DATE,
                    ultima_hora_venda_SilverSTGN_Dedup TIME,
                    UNIQUE (associacao, cod_farmacia)
                );

                CREATE TABLE IF NOT EXISTS divergencias (
                    id SERIAL PRIMARY KEY,
                    associacao VARCHAR(100) NOT NULL,
                    comparacao_id INTEGER REFERENCES comparacoes(id),
                    cod_farmacia VARCHAR(50) NOT NULL,
                    nome_farmacia VARCHAR(255),
                    cnpj VARCHAR(30),
                    ultima_venda_GoldVendas DATE,
                    ultima_hora_venda_GoldVendas TIMESTAMP,
                    ultima_venda_SilverSTGN_Dedup DATE,
                    ultima_hora_venda_SilverSTGN_Dedup TIME,
                    tipo_divergencia VARCHAR(50) NOT NULL,
                    coletor_novo TEXT,
                    UNIQUE (associacao, cod_farmacia)
                );

                CREATE TABLE IF NOT EXISTS status_farmacias (
                    id SERIAL PRIMARY KEY,
                    associacao VARCHAR(100) NOT NULL,
                    comparacao_id INTEGER REFERENCES comparacoes(id),
                    cod_farmacia VARCHAR(50) NOT NULL,
                    coletor_novo TEXT NOT NULL,
                    UNIQUE (associacao, cod_farmacia)
                );
            """)
        conn.commit()


def salvar_comparacao(
    associacao: str,
    resultados_gold_vendas: list[dict],
    resultados_silver_stgn_dedup: list[dict],
    divergencias: list[dict],
) -> int:
    """Salva os resultados da comparação no PostgreSQL local.

    comparacoes é append-only: cada rodada gera um novo ID (histórico de execuções).
    As tabelas filhas usam UPSERT em (associacao, cod_farmacia), preservando os IDs
    das farmácias entre rodadas. comparacao_id nas filhas aponta para a rodada mais recente.
    Farmácias que desapareceram do novo resultado são removidas.

    Args:
        associacao: Código da associação comparada
        resultados_gold_vendas: Lista de dicts com resultados de associacao.vendas
        resultados_silver_stgn_dedup: Lista de dicts com resultados de silver.cadcvend_staging_dedup
        divergencias: Lista de dicts com as divergências encontradas

    Returns:
        int: ID da nova comparação criada em comparacoes
    """
    with get_local_connection() as conn:
        with conn.cursor() as cur:
            # Nova linha a cada rodada — histórico de execuções
            cur.execute("""
                INSERT INTO comparacoes (associacao, total_gold_vendas, total_silver_stgn_dedup, total_divergencias)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """, (associacao, len(resultados_gold_vendas), len(resultados_silver_stgn_dedup), len(divergencias)))
            comparacao_id = cur.fetchone()[0]

            # --- resultados_gold_vendas ---
            novos_gold = {str(r["cod_farmacia"]).strip() for r in resultados_gold_vendas}
            if novos_gold:
                cur.execute(
                    "DELETE FROM resultados_gold_vendas WHERE associacao = %s AND cod_farmacia <> ALL(%s)",
                    (associacao, list(novos_gold)),
                )
            else:
                cur.execute("DELETE FROM resultados_gold_vendas WHERE associacao = %s", (associacao,))
            for r in resultados_gold_vendas:
                cur.execute("""
                    INSERT INTO resultados_gold_vendas
                        (associacao, comparacao_id, cod_farmacia, nome_farmacia, cnpj, ultima_venda, ultima_hora_venda)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (associacao, cod_farmacia) DO UPDATE
                        SET comparacao_id     = EXCLUDED.comparacao_id,
                            nome_farmacia     = EXCLUDED.nome_farmacia,
                            cnpj              = EXCLUDED.cnpj,
                            ultima_venda      = EXCLUDED.ultima_venda,
                            ultima_hora_venda = EXCLUDED.ultima_hora_venda
                """, (associacao, comparacao_id, r["cod_farmacia"], r.get("nome_farmacia"), r.get("cnpj"), r.get("ultima_venda"), r.get("ultima_hora_venda")))

            # --- resultados_silver_stgn_dedup ---
            novos_silver = {str(r["cod_farmacia"]).strip() for r in resultados_silver_stgn_dedup}
            if novos_silver:
                cur.execute(
                    "DELETE FROM resultados_silver_stgn_dedup WHERE associacao = %s AND cod_farmacia <> ALL(%s)",
                    (associacao, list(novos_silver)),
                )
            else:
                cur.execute("DELETE FROM resultados_silver_stgn_dedup WHERE associacao = %s", (associacao,))
            for r in resultados_silver_stgn_dedup:
                cur.execute("""
                    INSERT INTO resultados_silver_stgn_dedup
                        (associacao, comparacao_id, cod_farmacia, ultima_venda, ultima_hora_venda)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (associacao, cod_farmacia) DO UPDATE
                        SET comparacao_id     = EXCLUDED.comparacao_id,
                            ultima_venda      = EXCLUDED.ultima_venda,
                            ultima_hora_venda = EXCLUDED.ultima_hora_venda
                """, (associacao, comparacao_id, r["cod_farmacia"], r.get("ultima_venda"), r.get("ultima_hora_venda")))

            # --- resultados_consolidados ---
            gold_by_cod = {str(r["cod_farmacia"]).strip(): r for r in resultados_gold_vendas}
            silver_by_cod = {str(r["cod_farmacia"]).strip(): r for r in resultados_silver_stgn_dedup}
            todas_farmacias = set(gold_by_cod.keys()) | set(silver_by_cod.keys())
            div_lookup = {str(d["cod_farmacia"]).strip(): d for d in divergencias}

            if todas_farmacias:
                cur.execute(
                    "DELETE FROM resultados_consolidados WHERE associacao = %s AND cod_farmacia <> ALL(%s)",
                    (associacao, list(todas_farmacias)),
                )
            else:
                cur.execute("DELETE FROM resultados_consolidados WHERE associacao = %s", (associacao,))
            for cod in todas_farmacias:
                r_gold = gold_by_cod.get(cod)
                r_silver = silver_by_cod.get(cod)
                nome = r_gold.get("nome_farmacia") if r_gold else None
                cnpj = r_gold.get("cnpj") if r_gold else None
                if not nome or not cnpj:
                    div = div_lookup.get(cod, {})
                    nome = nome or div.get("nome_farmacia")
                    cnpj = cnpj or div.get("cnpj")
                cur.execute("""
                    INSERT INTO resultados_consolidados
                        (associacao, comparacao_id, cod_farmacia, nome_farmacia, cnpj,
                         ultima_venda_GoldVendas, ultima_hora_venda_GoldVendas,
                         ultima_venda_SilverSTGN_Dedup, ultima_hora_venda_SilverSTGN_Dedup)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (associacao, cod_farmacia) DO UPDATE
                        SET comparacao_id                      = EXCLUDED.comparacao_id,
                            nome_farmacia                      = EXCLUDED.nome_farmacia,
                            cnpj                               = EXCLUDED.cnpj,
                            ultima_venda_GoldVendas            = EXCLUDED.ultima_venda_GoldVendas,
                            ultima_hora_venda_GoldVendas       = EXCLUDED.ultima_hora_venda_GoldVendas,
                            ultima_venda_SilverSTGN_Dedup      = EXCLUDED.ultima_venda_SilverSTGN_Dedup,
                            ultima_hora_venda_SilverSTGN_Dedup = EXCLUDED.ultima_hora_venda_SilverSTGN_Dedup
                """, (
                    associacao, comparacao_id, cod, nome, _sanitize_cnpj(cnpj),
                    r_gold.get("ultima_venda") if r_gold else None,
                    r_gold.get("ultima_hora_venda") if r_gold else None,
                    r_silver.get("ultima_venda") if r_silver else None,
                    r_silver.get("ultima_hora_venda") if r_silver else None,
                ))

            # --- divergencias ---
            novos_divs = {str(d["cod_farmacia"]).strip() for d in divergencias}
            if novos_divs:
                cur.execute(
                    "DELETE FROM divergencias WHERE associacao = %s AND cod_farmacia <> ALL(%s)",
                    (associacao, list(novos_divs)),
                )
            else:
                cur.execute("DELETE FROM divergencias WHERE associacao = %s", (associacao,))
            for d in divergencias:
                cur.execute("""
                    INSERT INTO divergencias
                        (associacao, comparacao_id, cod_farmacia, nome_farmacia, cnpj,
                         ultima_venda_GoldVendas, ultima_hora_venda_GoldVendas,
                         ultima_venda_SilverSTGN_Dedup, ultima_hora_venda_SilverSTGN_Dedup, tipo_divergencia)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (associacao, cod_farmacia) DO UPDATE
                        SET comparacao_id                      = EXCLUDED.comparacao_id,
                            nome_farmacia                      = EXCLUDED.nome_farmacia,
                            cnpj                               = EXCLUDED.cnpj,
                            ultima_venda_GoldVendas            = EXCLUDED.ultima_venda_GoldVendas,
                            ultima_hora_venda_GoldVendas       = EXCLUDED.ultima_hora_venda_GoldVendas,
                            ultima_venda_SilverSTGN_Dedup      = EXCLUDED.ultima_venda_SilverSTGN_Dedup,
                            ultima_hora_venda_SilverSTGN_Dedup = EXCLUDED.ultima_hora_venda_SilverSTGN_Dedup,
                            tipo_divergencia                   = EXCLUDED.tipo_divergencia
                """, (
                    associacao, comparacao_id, d["cod_farmacia"], d.get("nome_farmacia"), d.get("cnpj"),
                    d.get("ultima_venda_GoldVendas"), d.get("ultima_hora_venda_GoldVendas"),
                    d.get("ultima_venda_SilverSTGN_Dedup"), d.get("ultima_hora_venda_SilverSTGN_Dedup"),
                    d["tipo_divergencia"],
                ))

        conn.commit()
        return comparacao_id


def buscar_ultima_atualizacao() -> Optional[str]:
    """Retorna a data/hora da comparação mais recente entre todas as associações."""
    with get_local_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(executado_em)::text FROM comparacoes")
            row = cur.fetchone()
            return row[0] if row else None


def buscar_todos_consolidados() -> list[dict]:
    """Retorna todas as farmácias de todas as associações (dados da última rodada de cada uma).

    Como as tabelas filhas têm UNIQUE (associacao, cod_farmacia), cada farmácia tem
    exatamente uma linha, sempre atualizada com os dados mais recentes.

    Returns:
        Lista com todas as farmácias, incluindo associacao, coletor_novo e tipo_divergencia
    """
    with get_local_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    rc.associacao,
                    rc.cod_farmacia,
                    rc.nome_farmacia,
                    rc.cnpj,
                    rc.ultima_venda_GoldVendas::text,
                    rc.ultima_hora_venda_GoldVendas::text,
                    rc.ultima_venda_SilverSTGN_Dedup::text,
                    rc.ultima_hora_venda_SilverSTGN_Dedup::text,
                    sf.coletor_novo,
                    d.tipo_divergencia,
                    c.executado_em::text AS atualizado_em
                FROM resultados_consolidados rc
                LEFT JOIN status_farmacias sf
                    ON sf.associacao = rc.associacao AND sf.cod_farmacia = rc.cod_farmacia
                LEFT JOIN divergencias d
                    ON d.associacao = rc.associacao AND d.cod_farmacia = rc.cod_farmacia
                LEFT JOIN comparacoes c
                    ON c.id = rc.comparacao_id
                ORDER BY rc.associacao, rc.cod_farmacia
            """)
            return [dict(row) for row in cur.fetchall()]


def buscar_consolidado_por_associacao(associacao: str) -> list[dict]:
    """Busca os resultados consolidados da última rodada de uma associação.

    Args:
        associacao: Código da associação

    Returns:
        Lista de registros com dados de ambas as fontes, status do coletor e tipo de divergência
    """
    with get_local_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    rc.cod_farmacia,
                    rc.nome_farmacia,
                    rc.cnpj,
                    rc.ultima_venda_GoldVendas::text,
                    rc.ultima_hora_venda_GoldVendas::text,
                    rc.ultima_venda_SilverSTGN_Dedup::text,
                    rc.ultima_hora_venda_SilverSTGN_Dedup::text,
                    sf.coletor_novo,
                    d.tipo_divergencia,
                    c.executado_em::text AS atualizado_em
                FROM resultados_consolidados rc
                LEFT JOIN status_farmacias sf
                    ON sf.associacao = rc.associacao AND sf.cod_farmacia = rc.cod_farmacia
                LEFT JOIN divergencias d
                    ON d.associacao = rc.associacao AND d.cod_farmacia = rc.cod_farmacia
                LEFT JOIN comparacoes c
                    ON c.id = rc.comparacao_id
                WHERE rc.associacao = %s
                ORDER BY rc.cod_farmacia
            """, (associacao,))
            return [dict(row) for row in cur.fetchall()]


def salvar_status_farmacias(comparacao_id: int, associacao: str, status_farmacias: dict[str, str]) -> None:
    """Persiste o status de migração das farmácias no PostgreSQL local."""
    if not status_farmacias:
        return

    with get_local_connection() as conn:
        with conn.cursor() as cur:
            for cod_farmacia, coletor_novo in status_farmacias.items():
                cur.execute("""
                    INSERT INTO status_farmacias (associacao, comparacao_id, cod_farmacia, coletor_novo)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (associacao, cod_farmacia) DO UPDATE
                        SET comparacao_id = EXCLUDED.comparacao_id,
                            coletor_novo  = EXCLUDED.coletor_novo
                """, (associacao, comparacao_id, cod_farmacia, coletor_novo))

                cur.execute("""
                    UPDATE divergencias
                    SET coletor_novo = %s
                    WHERE associacao = %s AND cod_farmacia = %s
                """, (coletor_novo, associacao, cod_farmacia))

        conn.commit()

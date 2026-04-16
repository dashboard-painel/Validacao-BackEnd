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
                    comparacao_id INTEGER REFERENCES comparacoes(id) ON DELETE CASCADE,
                    cod_farmacia VARCHAR(50) NOT NULL,
                    nome_farmacia VARCHAR(255),
                    cnpj VARCHAR(30),
                    ultima_venda DATE,
                    ultima_hora_venda TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS resultados_silver_stgn_dedup (
                    id SERIAL PRIMARY KEY,
                    comparacao_id INTEGER REFERENCES comparacoes(id) ON DELETE CASCADE,
                    cod_farmacia VARCHAR(50) NOT NULL,
                    ultima_venda DATE,
                    ultima_hora_venda TIME
                );

                CREATE TABLE IF NOT EXISTS resultados_consolidados (
                    id SERIAL PRIMARY KEY,
                    comparacao_id INTEGER REFERENCES comparacoes(id) ON DELETE CASCADE,
                    cod_farmacia VARCHAR(50) NOT NULL,
                    nome_farmacia VARCHAR(255),
                    cnpj VARCHAR(30),
                    ultima_venda_GoldVendas DATE,
                    ultima_hora_venda_GoldVendas TIMESTAMP,
                    ultima_venda_SilverSTGN_Dedup DATE,
                    ultima_hora_venda_SilverSTGN_Dedup TIME
                );

                CREATE TABLE IF NOT EXISTS divergencias (
                    id SERIAL PRIMARY KEY,
                    comparacao_id INTEGER REFERENCES comparacoes(id) ON DELETE CASCADE,
                    cod_farmacia VARCHAR(50) NOT NULL,
                    nome_farmacia VARCHAR(255),
                    cnpj VARCHAR(30),
                    ultima_venda_GoldVendas DATE,
                    ultima_hora_venda_GoldVendas TIMESTAMP,
                    ultima_venda_SilverSTGN_Dedup DATE,
                    ultima_hora_venda_SilverSTGN_Dedup TIME,
                    tipo_divergencia VARCHAR(50) NOT NULL,
                    coletor_novo TEXT
                );

                CREATE TABLE IF NOT EXISTS status_farmacias (
                    id SERIAL PRIMARY KEY,
                    comparacao_id INTEGER REFERENCES comparacoes(id) ON DELETE CASCADE,
                    cod_farmacia VARCHAR(50) NOT NULL,
                    coletor_novo TEXT NOT NULL
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

    Cada chamada gera um novo registro em comparacoes (log imutável).

    Args:
        associacao: Código da associação comparada
        resultados_gold_vendas: Lista de dicts com resultados de associacao.vendas
        resultados_silver_stgn_dedup: Lista de dicts com resultados de silver.cadcvend_staging_dedup
        divergencias: Lista de dicts com as divergências encontradas

    Returns:
        int: ID da comparação criada na tabela comparacoes
    """
    with get_local_connection() as conn:
        with conn.cursor() as cur:
            # Remove execuções anteriores da mesma associação (delete em cascata manual)
            cur.execute("""
                DELETE FROM resultados_gold_vendas
                WHERE comparacao_id IN (SELECT id FROM comparacoes WHERE associacao = %s)
            """, (associacao,))
            cur.execute("""
                DELETE FROM resultados_silver_stgn_dedup
                WHERE comparacao_id IN (SELECT id FROM comparacoes WHERE associacao = %s)
            """, (associacao,))
            cur.execute("""
                DELETE FROM resultados_consolidados
                WHERE comparacao_id IN (SELECT id FROM comparacoes WHERE associacao = %s)
            """, (associacao,))
            cur.execute("""
                DELETE FROM divergencias
                WHERE comparacao_id IN (SELECT id FROM comparacoes WHERE associacao = %s)
            """, (associacao,))
            cur.execute("""
                DELETE FROM status_farmacias
                WHERE comparacao_id IN (SELECT id FROM comparacoes WHERE associacao = %s)
            """, (associacao,))
            cur.execute("""
                DELETE FROM comparacoes WHERE associacao = %s
            """, (associacao,))

            cur.execute("""
                INSERT INTO comparacoes (associacao, total_gold_vendas, total_silver_stgn_dedup, total_divergencias)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """, (associacao, len(resultados_gold_vendas), len(resultados_silver_stgn_dedup), len(divergencias)))
            comparacao_id = cur.fetchone()[0]

            for r in resultados_gold_vendas:
                cur.execute("""
                    INSERT INTO resultados_gold_vendas (comparacao_id, cod_farmacia, nome_farmacia, cnpj, ultima_venda, ultima_hora_venda)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (comparacao_id, r["cod_farmacia"], r.get("nome_farmacia"), r.get("cnpj"), r.get("ultima_venda"), r.get("ultima_hora_venda")))

            for r in resultados_silver_stgn_dedup:
                cur.execute("""
                    INSERT INTO resultados_silver_stgn_dedup (comparacao_id, cod_farmacia, ultima_venda, ultima_hora_venda)
                    VALUES (%s, %s, %s, %s)
                """, (comparacao_id, r["cod_farmacia"], r.get("ultima_venda"), r.get("ultima_hora_venda")))

            # Consolida GoldVendas e SilverSTGN_Dedup (FULL OUTER JOIN por cod_farmacia)
            gold_by_cod = {str(r["cod_farmacia"]).strip(): r for r in resultados_gold_vendas}
            silver_by_cod = {str(r["cod_farmacia"]).strip(): r for r in resultados_silver_stgn_dedup}
            todas_farmacias = set(gold_by_cod.keys()) | set(silver_by_cod.keys())

            # Lookup de nome/cnpj das divergências (já enriquecidas pelo cadfilia no comparador)
            div_lookup = {str(d["cod_farmacia"]).strip(): d for d in divergencias}

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
                        (comparacao_id, cod_farmacia, nome_farmacia, cnpj,
                         ultima_venda_GoldVendas, ultima_hora_venda_GoldVendas,
                         ultima_venda_SilverSTGN_Dedup, ultima_hora_venda_SilverSTGN_Dedup)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    comparacao_id, cod, nome, _sanitize_cnpj(cnpj),
                    r_gold.get("ultima_venda") if r_gold else None,
                    r_gold.get("ultima_hora_venda") if r_gold else None,
                    r_silver.get("ultima_venda") if r_silver else None,
                    r_silver.get("ultima_hora_venda") if r_silver else None,
                ))

            for d in divergencias:
                cur.execute("""
                    INSERT INTO divergencias (comparacao_id, cod_farmacia, nome_farmacia, cnpj,
                        ultima_venda_GoldVendas, ultima_hora_venda_GoldVendas,
                        ultima_venda_SilverSTGN_Dedup, ultima_hora_venda_SilverSTGN_Dedup, tipo_divergencia)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    comparacao_id, d["cod_farmacia"], d.get("nome_farmacia"), d.get("cnpj"),
                    d.get("ultima_venda_GoldVendas"), d.get("ultima_hora_venda_GoldVendas"),
                    d.get("ultima_venda_SilverSTGN_Dedup"), d.get("ultima_hora_venda_SilverSTGN_Dedup"),
                    d["tipo_divergencia"],
                ))

        conn.commit()
        return comparacao_id


def buscar_todos_consolidados() -> list[dict]:
    """Retorna todas as farmácias de todas as associações (última comparação de cada uma).

    Returns:
        Lista com todas as farmácias, incluindo associacao, coletor_novo e tipo_divergencia
    """
    with get_local_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    c.associacao,
                    rc.cod_farmacia,
                    rc.nome_farmacia,
                    rc.cnpj,
                    rc.ultima_venda_GoldVendas::text,
                    rc.ultima_hora_venda_GoldVendas::text,
                    rc.ultima_venda_SilverSTGN_Dedup::text,
                    rc.ultima_hora_venda_SilverSTGN_Dedup::text,
                    sf.coletor_novo,
                    d.tipo_divergencia
                FROM resultados_consolidados rc
                JOIN comparacoes c ON c.id = rc.comparacao_id
                LEFT JOIN (
                    SELECT DISTINCT ON (comparacao_id, cod_farmacia) comparacao_id, cod_farmacia, coletor_novo
                    FROM status_farmacias
                    ORDER BY comparacao_id, cod_farmacia, id DESC
                ) sf ON sf.comparacao_id = rc.comparacao_id AND sf.cod_farmacia = rc.cod_farmacia
                LEFT JOIN (
                    SELECT DISTINCT ON (comparacao_id, cod_farmacia) comparacao_id, cod_farmacia, tipo_divergencia
                    FROM divergencias
                    ORDER BY comparacao_id, cod_farmacia, id DESC
                ) d ON d.comparacao_id = rc.comparacao_id AND d.cod_farmacia = rc.cod_farmacia
                WHERE c.id IN (
                    SELECT DISTINCT ON (associacao) id
                    FROM comparacoes
                    ORDER BY associacao, executado_em DESC
                )
                ORDER BY c.associacao, rc.cod_farmacia
            """)
            return [dict(row) for row in cur.fetchall()]


def buscar_consolidado_por_associacao(associacao: str) -> list[dict]:
    """Busca os resultados consolidados da comparação mais recente de uma associação.

    Faz JOIN com status_farmacias (coletor_novo) e divergencias (tipo_divergencia).

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
                    d.tipo_divergencia
                FROM resultados_consolidados rc
                JOIN comparacoes c ON c.id = rc.comparacao_id
                LEFT JOIN (
                    SELECT DISTINCT ON (comparacao_id, cod_farmacia) comparacao_id, cod_farmacia, coletor_novo
                    FROM status_farmacias
                    ORDER BY comparacao_id, cod_farmacia, id DESC
                ) sf ON sf.comparacao_id = rc.comparacao_id AND sf.cod_farmacia = rc.cod_farmacia
                LEFT JOIN (
                    SELECT DISTINCT ON (comparacao_id, cod_farmacia) comparacao_id, cod_farmacia, tipo_divergencia
                    FROM divergencias
                    ORDER BY comparacao_id, cod_farmacia, id DESC
                ) d ON d.comparacao_id = rc.comparacao_id AND d.cod_farmacia = rc.cod_farmacia
                WHERE c.associacao = %s
                  AND c.id = (
                      SELECT id FROM comparacoes
                      WHERE associacao = %s
                      ORDER BY executado_em DESC
                      LIMIT 1
                  )
                ORDER BY rc.cod_farmacia
            """, (associacao, associacao))
            return [dict(row) for row in cur.fetchall()]


def salvar_status_farmacias(comparacao_id: int, status_farmacias: dict[str, str]) -> None:
    """Persiste o status de migração das farmácias no PostgreSQL local."""
    if not status_farmacias:
        return

    with get_local_connection() as conn:
        with conn.cursor() as cur:
            for cod_farmacia, coletor_novo in status_farmacias.items():
                cur.execute("""
                    INSERT INTO status_farmacias (comparacao_id, cod_farmacia, coletor_novo)
                    VALUES (%s, %s, %s)
                """, (comparacao_id, cod_farmacia, coletor_novo))

                cur.execute("""
                    UPDATE divergencias
                    SET coletor_novo = %s
                    WHERE comparacao_id = %s AND cod_farmacia = %s
                """, (coletor_novo, comparacao_id, cod_farmacia))

        conn.commit()

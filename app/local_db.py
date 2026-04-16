"""Módulo de persistência local no PostgreSQL para resultados de comparação."""
import os
from datetime import datetime
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()


def get_local_connection():
    """Retorna conexão com o PostgreSQL local.

    Suporta dois formatos no .env:
    - LOCAL_DB_URL=postgresql://user:pass@host:port/db  (formato psycopg2)
    - LOCAL_DB_URL=jdbc:postgresql://host:port/db + LOCAL_DB_USER + LOCAL_DB_PASS
    """
    url = os.getenv("LOCAL_DB_URL", "")
    # Remove prefixo jdbc: se presente
    url = url.replace("jdbc:", "", 1)

    user = os.getenv("LOCAL_DB_USER")
    password = os.getenv("LOCAL_DB_PASS")

    # Injeta user:pass na URL se não estiverem presentes e env vars existirem
    if user and password and "@" not in url:
        # postgresql://host:port/db → postgresql://user:pass@host:port/db
        url = url.replace("postgresql://", f"postgresql://{user}:{password}@", 1)

    return psycopg2.connect(url)


def init_local_db():
    """Cria as tabelas necessárias no PostgreSQL local se não existirem."""
    with get_local_connection() as conn:
        with conn.cursor() as cur:
            # Migra tabelas e colunas legadas para os novos nomes descritivos
            cur.execute("""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public' AND table_name = 'resultados_q1'
                    ) THEN
                        ALTER TABLE resultados_q1 RENAME TO resultados_gold_vendas;
                    END IF;

                    IF EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public' AND table_name = 'resultados_q2'
                    ) THEN
                        ALTER TABLE resultados_q2 RENAME TO resultados_silver_stgn_dedup;
                    END IF;

                    IF EXISTS (
                        SELECT FROM information_schema.columns
                        WHERE table_schema = 'public' AND table_name = 'comparacoes' AND column_name = 'total_q1'
                    ) THEN
                        ALTER TABLE comparacoes RENAME COLUMN total_q1 TO total_gold_vendas;
                    END IF;

                    IF EXISTS (
                        SELECT FROM information_schema.columns
                        WHERE table_schema = 'public' AND table_name = 'comparacoes' AND column_name = 'total_q2'
                    ) THEN
                        ALTER TABLE comparacoes RENAME COLUMN total_q2 TO total_silver_stgn_dedup;
                    END IF;
                END $$;
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS comparacoes (
                    id SERIAL PRIMARY KEY,
                    associacao VARCHAR(100) NOT NULL,
                    dat_emissao_filtro DATE NOT NULL,
                    executado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    total_gold_vendas INTEGER NOT NULL,
                    total_silver_stgn_dedup INTEGER NOT NULL,
                    total_divergencias INTEGER NOT NULL
                );

                -- Resultados de associacao.vendas (GoldVendas)
                CREATE TABLE IF NOT EXISTS resultados_gold_vendas (
                    id SERIAL PRIMARY KEY,
                    comparacao_id INTEGER REFERENCES comparacoes(id),
                    cod_farmacia VARCHAR(50) NOT NULL,
                    nome_farmacia VARCHAR(255),
                    ultima_venda DATE,
                    ultima_hora_venda TIMESTAMP
                );

                -- Resultados de silver.cadcvend_staging_dedup (SilverSTGN_Dedup)
                CREATE TABLE IF NOT EXISTS resultados_silver_stgn_dedup (
                    id SERIAL PRIMARY KEY,
                    comparacao_id INTEGER REFERENCES comparacoes(id),
                    cod_farmacia VARCHAR(50) NOT NULL,
                    ultima_venda DATE,
                    ultima_hora_venda TIME
                );

                -- JOIN consolidado de GoldVendas e SilverSTGN_Dedup por cod_farmacia
                CREATE TABLE IF NOT EXISTS resultados_consolidados (
                    id SERIAL PRIMARY KEY,
                    comparacao_id INTEGER REFERENCES comparacoes(id),
                    cod_farmacia VARCHAR(50) NOT NULL,
                    nome_farmacia VARCHAR(255),
                    ultima_venda_GoldVendas DATE,
                    ultima_hora_venda_GoldVendas TIMESTAMP,
                    ultima_venda_SilverSTGN_Dedup DATE,
                    ultima_hora_venda_SilverSTGN_Dedup TIME
                );

                CREATE TABLE IF NOT EXISTS divergencias (
                    id SERIAL PRIMARY KEY,
                    comparacao_id INTEGER REFERENCES comparacoes(id),
                    cod_farmacia VARCHAR(50) NOT NULL,
                    nome_farmacia VARCHAR(255),
                    ultima_venda_GoldVendas DATE,
                    ultima_hora_venda_GoldVendas TIMESTAMP,
                    ultima_venda_SilverSTGN_Dedup DATE,
                    ultima_hora_venda_SilverSTGN_Dedup TIME,
                    tipo_divergencia VARCHAR(50) NOT NULL
                );

                ALTER TABLE divergencias ADD COLUMN IF NOT EXISTS coletor_novo TEXT;

                CREATE TABLE IF NOT EXISTS status_farmacias (
                    id SERIAL PRIMARY KEY,
                    comparacao_id INTEGER REFERENCES comparacoes(id),
                    cod_farmacia VARCHAR(50) NOT NULL,
                    coletor_novo TEXT NOT NULL
                );
            """)
        conn.commit()


def salvar_comparacao(
    associacao: str,
    dat_emissao_filtro: str,
    resultados_gold_vendas: list[dict],
    resultados_silver_stgn_dedup: list[dict],
    divergencias: list[dict],
    nomes_lookup: dict[str, str] | None = None,
) -> int:
    """Salva os resultados da comparação no PostgreSQL local.

    Persiste o registro principal da comparação e todos os resultados de
    GoldVendas, SilverSTGN_Dedup e as divergências identificadas em tabelas separadas.

    Args:
        associacao: Código da associação comparada
        dat_emissao_filtro: Data de emissão mínima usada como filtro (YYYY-MM-DD)
        resultados_gold_vendas: Lista de dicts com resultados de associacao.vendas
        resultados_silver_stgn_dedup: Lista de dicts com resultados de silver.cadcvend_staging_dedup
        divergencias: Lista de dicts com as divergências encontradas
        nomes_lookup: Dict {cod_farmacia: nome_farmacia} para farmácias sem nome em GoldVendas

    Returns:
        int: ID da comparação criada na tabela comparacoes
    """
    with get_local_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO comparacoes (associacao, dat_emissao_filtro, total_gold_vendas, total_silver_stgn_dedup, total_divergencias)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, (associacao, dat_emissao_filtro, len(resultados_gold_vendas), len(resultados_silver_stgn_dedup), len(divergencias)))
            comparacao_id = cur.fetchone()[0]

            for r in resultados_gold_vendas:
                cur.execute("""
                    INSERT INTO resultados_gold_vendas (comparacao_id, cod_farmacia, nome_farmacia, ultima_venda, ultima_hora_venda)
                    VALUES (%s, %s, %s, %s, %s)
                """, (comparacao_id, r["cod_farmacia"], r.get("nome_farmacia"), r.get("ultima_venda"), r.get("ultima_hora_venda")))

            for r in resultados_silver_stgn_dedup:
                cur.execute("""
                    INSERT INTO resultados_silver_stgn_dedup (comparacao_id, cod_farmacia, ultima_venda, ultima_hora_venda)
                    VALUES (%s, %s, %s, %s)
                """, (comparacao_id, r["cod_farmacia"], r.get("ultima_venda"), r.get("ultima_hora_venda")))

            # Consolida GoldVendas e SilverSTGN_Dedup lado a lado (FULL OUTER JOIN por cod_farmacia)
            gold_by_cod = {str(r["cod_farmacia"]): r for r in resultados_gold_vendas}
            silver_by_cod = {str(r["cod_farmacia"]): r for r in resultados_silver_stgn_dedup}
            todas_farmacias = set(gold_by_cod.keys()) | set(silver_by_cod.keys())
            nomes_lookup = nomes_lookup or {}

            for cod in todas_farmacias:
                r_gold = gold_by_cod.get(cod)
                r_silver = silver_by_cod.get(cod)
                nome = (r_gold.get("nome_farmacia") if r_gold else None) or nomes_lookup.get(cod)
                cur.execute("""
                    INSERT INTO resultados_consolidados
                        (comparacao_id, cod_farmacia, nome_farmacia,
                         ultima_venda_GoldVendas, ultima_hora_venda_GoldVendas,
                         ultima_venda_SilverSTGN_Dedup, ultima_hora_venda_SilverSTGN_Dedup)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    comparacao_id,
                    cod,
                    nome,
                    r_gold.get("ultima_venda") if r_gold else None,
                    r_gold.get("ultima_hora_venda") if r_gold else None,
                    r_silver.get("ultima_venda") if r_silver else None,
                    r_silver.get("ultima_hora_venda") if r_silver else None,
                ))

            for d in divergencias:
                cur.execute("""
                    INSERT INTO divergencias (comparacao_id, cod_farmacia, nome_farmacia, ultima_venda_GoldVendas, ultima_hora_venda_GoldVendas, ultima_venda_SilverSTGN_Dedup, ultima_hora_venda_SilverSTGN_Dedup, tipo_divergencia)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (comparacao_id, d["cod_farmacia"], d.get("nome_farmacia"), d.get("ultima_venda_GoldVendas"), d.get("ultima_hora_venda_GoldVendas"), d.get("ultima_venda_SilverSTGN_Dedup"), d.get("ultima_hora_venda_SilverSTGN_Dedup"), d["tipo_divergencia"]))

        conn.commit()
        return comparacao_id


def buscar_resultados_consolidados(comparacao_id: int) -> list[dict]:
    """Busca os resultados consolidados (GoldVendas + SilverSTGN_Dedup) de uma comparação no PostgreSQL local.

    Args:
        comparacao_id: ID da comparação na tabela comparacoes

    Returns:
        Lista de dicts com colunas: cod_farmacia, nome_farmacia,
        ultima_venda_GoldVendas, ultima_hora_venda_GoldVendas,
        ultima_venda_SilverSTGN_Dedup, ultima_hora_venda_SilverSTGN_Dedup
    """
    with get_local_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    cod_farmacia,
                    nome_farmacia,
                    ultima_venda_GoldVendas::text,
                    ultima_hora_venda_GoldVendas::text,
                    ultima_venda_SilverSTGN_Dedup::text,
                    ultima_hora_venda_SilverSTGN_Dedup::text
                FROM resultados_consolidados
                WHERE comparacao_id = %s
                ORDER BY cod_farmacia
            """, (comparacao_id,))
            return [dict(row) for row in cur.fetchall()]


def salvar_status_farmacias(comparacao_id: int, status_farmacias: dict[str, str]) -> None:
    """Persiste o status de migração das farmácias no PostgreSQL local.

    Para cada farmácia em status_farmacias:
    1. Insere na tabela status_farmacias
    2. Atualiza coletor_novo na tabela divergencias (se a farmácia tiver divergência)

    Args:
        comparacao_id: ID da comparação na tabela comparacoes
        status_farmacias: Dict {cod_farmacia: coletor_novo}
    """
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

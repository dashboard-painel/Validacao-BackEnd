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
            cur.execute("""
                CREATE TABLE IF NOT EXISTS comparacoes (
                    id SERIAL PRIMARY KEY,
                    associacao VARCHAR(100) NOT NULL,
                    dat_emissao_filtro DATE NOT NULL,
                    executado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    total_q1 INTEGER NOT NULL,
                    total_q2 INTEGER NOT NULL,
                    total_divergencias INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS resultados_q1 (
                    id SERIAL PRIMARY KEY,
                    comparacao_id INTEGER REFERENCES comparacoes(id),
                    cod_farmacia VARCHAR(50) NOT NULL,
                    nome_farmacia VARCHAR(255),
                    ultima_venda DATE,
                    ultima_hora_venda TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS resultados_q2 (
                    id SERIAL PRIMARY KEY,
                    comparacao_id INTEGER REFERENCES comparacoes(id),
                    cod_farmacia VARCHAR(50) NOT NULL,
                    ultima_venda DATE,
                    ultima_hora_venda TIME
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
    resultados_q1: list[dict],
    resultados_q2: list[dict],
    divergencias: list[dict]
) -> int:
    """Salva os resultados da comparação no PostgreSQL local.

    Persiste o registro principal da comparação e todos os resultados de Q1,
    Q2 e as divergências identificadas em tabelas separadas.

    Args:
        associacao: Código da associação comparada
        dat_emissao_filtro: Data de emissão mínima usada como filtro (YYYY-MM-DD)
        resultados_q1: Lista de dicts com resultados da Query 1
        resultados_q2: Lista de dicts com resultados da Query 2
        divergencias: Lista de dicts com as divergências encontradas

    Returns:
        int: ID da comparação criada na tabela comparacoes
    """
    with get_local_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO comparacoes (associacao, dat_emissao_filtro, total_q1, total_q2, total_divergencias)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, (associacao, dat_emissao_filtro, len(resultados_q1), len(resultados_q2), len(divergencias)))
            comparacao_id = cur.fetchone()[0]

            for r in resultados_q1:
                cur.execute("""
                    INSERT INTO resultados_q1 (comparacao_id, cod_farmacia, nome_farmacia, ultima_venda, ultima_hora_venda)
                    VALUES (%s, %s, %s, %s, %s)
                """, (comparacao_id, r["cod_farmacia"], r.get("nome_farmacia"), r.get("ultima_venda"), r.get("ultima_hora_venda")))

            for r in resultados_q2:
                cur.execute("""
                    INSERT INTO resultados_q2 (comparacao_id, cod_farmacia, ultima_venda, ultima_hora_venda)
                    VALUES (%s, %s, %s, %s)
                """, (comparacao_id, r["cod_farmacia"], r.get("ultima_venda"), r.get("ultima_hora_venda")))

            for d in divergencias:
                cur.execute("""
                    INSERT INTO divergencias (comparacao_id, cod_farmacia, nome_farmacia, ultima_venda_GoldVendas, ultima_hora_venda_GoldVendas, ultima_venda_SilverSTGN_Dedup, ultima_hora_venda_SilverSTGN_Dedup, tipo_divergencia)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (comparacao_id, d["cod_farmacia"], d.get("nome_farmacia"), d.get("ultima_venda_GoldVendas"), d.get("ultima_hora_venda_GoldVendas"), d.get("ultima_venda_SilverSTGN_Dedup"), d.get("ultima_hora_venda_SilverSTGN_Dedup"), d["tipo_divergencia"]))

        conn.commit()
        return comparacao_id


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

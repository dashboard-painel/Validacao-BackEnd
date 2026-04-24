"""Módulo de persistência local no PostgreSQL para resultados de comparação."""
import os
import re
from typing import Optional
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import RealDictCursor


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

                -- Dados brutos de cada fonte (mantidos para auditoria/debug)
                CREATE TABLE IF NOT EXISTS resultados_gold_vendas (
                    id SERIAL PRIMARY KEY,
                    comparacao_id INTEGER REFERENCES comparacoes(id),
                    cod_farmacia VARCHAR(50) NOT NULL,
                    nome_farmacia VARCHAR(255),
                    cnpj VARCHAR(30),
                    sit_contrato VARCHAR(50),
                    codigo_rede VARCHAR(50),
                    ultima_venda DATE,
                    ultima_hora_venda TIMESTAMP,
                    num_versao VARCHAR(50),
                    UNIQUE (codigo_rede, cod_farmacia)
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

                -- Estado atual consolidado de cada farmácia: dados de ambas as fontes,
                -- tipo de divergência e status dos coletores (Business Connect + Coletor BI).
                CREATE TABLE IF NOT EXISTS farmacias (
                    id SERIAL PRIMARY KEY,
                    comparacao_id INTEGER REFERENCES comparacoes(id),
                    cod_farmacia VARCHAR(50) NOT NULL,
                    nome_farmacia VARCHAR(255),
                    cnpj VARCHAR(30),
                    sit_contrato VARCHAR(50),
                    codigo_rede VARCHAR(50),
                    ultima_venda_GoldVendas DATE,
                    ultima_hora_venda_GoldVendas TIMESTAMP,
                    ultima_venda_SilverSTGN_Dedup DATE,
                    ultima_hora_venda_SilverSTGN_Dedup TIME,
                    dat_hora_emissao_vendas_parceiros TIMESTAMP,
                    tipo_divergencia VARCHAR(50),
                    coletor_novo TEXT,
                    coletor_bi_ultima_data DATE,
                    coletor_bi_ultima_hora TIME,
                    num_versao VARCHAR(50),
                    UNIQUE (codigo_rede, cod_farmacia)
                );

                CREATE TABLE IF NOT EXISTS resultados_vendas_parceiros (
                    id SERIAL PRIMARY KEY,
                    associacao VARCHAR(100) NOT NULL,
                    cod_farmacia VARCHAR(50) NOT NULL,
                    nome_farmacia VARCHAR(255),
                    sit_contrato VARCHAR(50),
                    farmacia VARCHAR(100),
                    associacao_parceiro VARCHAR(100),
                    ultima_venda_parceiros TIMESTAMP,
                    atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (associacao, cod_farmacia)
                );
            """)
            # Migração: garante colunas e remove duplicação em tabelas já existentes
            cur.execute("ALTER TABLE resultados_gold_vendas ADD COLUMN IF NOT EXISTS sit_contrato VARCHAR(50)")
            cur.execute("ALTER TABLE resultados_gold_vendas ADD COLUMN IF NOT EXISTS codigo_rede VARCHAR(50)")
            cur.execute("ALTER TABLE resultados_gold_vendas DROP COLUMN IF EXISTS associacao")
            cur.execute("ALTER TABLE farmacias ADD COLUMN IF NOT EXISTS sit_contrato VARCHAR(50)")
            cur.execute("ALTER TABLE farmacias ADD COLUMN IF NOT EXISTS codigo_rede VARCHAR(50)")
            cur.execute("ALTER TABLE farmacias DROP COLUMN IF EXISTS associacao")
            cur.execute("ALTER TABLE farmacias ADD COLUMN IF NOT EXISTS classificacao VARCHAR(50)")
            cur.execute("ALTER TABLE farmacias ADD COLUMN IF NOT EXISTS dat_hora_emissao_vendas_parceiros TIMESTAMP")
            cur.execute("ALTER TABLE farmacias ADD COLUMN IF NOT EXISTS num_versao VARCHAR(50)")
            cur.execute("ALTER TABLE comparacoes ADD COLUMN IF NOT EXISTS total_vendas_parceiros INTEGER DEFAULT 0")
            # Migração: repurpose resultados_vendas_parceiros com novas colunas
            cur.execute("ALTER TABLE resultados_vendas_parceiros ADD COLUMN IF NOT EXISTS nome_farmacia VARCHAR(255)")
            cur.execute("ALTER TABLE resultados_vendas_parceiros ADD COLUMN IF NOT EXISTS sit_contrato VARCHAR(50)")
            cur.execute("ALTER TABLE resultados_vendas_parceiros ADD COLUMN IF NOT EXISTS farmacia VARCHAR(100)")
            cur.execute("ALTER TABLE resultados_vendas_parceiros ADD COLUMN IF NOT EXISTS associacao_parceiro VARCHAR(100)")
            cur.execute("ALTER TABLE resultados_vendas_parceiros ADD COLUMN IF NOT EXISTS ultima_venda_parceiros TIMESTAMP")
            cur.execute("ALTER TABLE resultados_vendas_parceiros ADD COLUMN IF NOT EXISTS atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            cur.execute("ALTER TABLE resultados_vendas_parceiros DROP COLUMN IF EXISTS comparacao_id")
            cur.execute("ALTER TABLE resultados_vendas_parceiros DROP COLUMN IF EXISTS dat_hora_emissao_vendas_parceiros")
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_resultados_gold_vendas_rede_cod
                ON resultados_gold_vendas (codigo_rede, cod_farmacia)
            """)
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_farmacias_rede_cod
                ON farmacias (codigo_rede, cod_farmacia)
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
                    "DELETE FROM resultados_gold_vendas WHERE codigo_rede = %s AND cod_farmacia <> ALL(%s)",
                    (associacao, list(novos_gold)),
                )
            else:
                cur.execute("DELETE FROM resultados_gold_vendas WHERE codigo_rede = %s", (associacao,))
            for r in resultados_gold_vendas:
                cur.execute("""
                    INSERT INTO resultados_gold_vendas
                        (comparacao_id, cod_farmacia, nome_farmacia, cnpj,
                         sit_contrato, codigo_rede, ultima_venda, ultima_hora_venda, num_versao)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (codigo_rede, cod_farmacia) DO UPDATE
                        SET comparacao_id     = EXCLUDED.comparacao_id,
                            nome_farmacia     = EXCLUDED.nome_farmacia,
                            cnpj              = EXCLUDED.cnpj,
                            sit_contrato      = EXCLUDED.sit_contrato,
                            codigo_rede       = EXCLUDED.codigo_rede,
                            ultima_venda      = EXCLUDED.ultima_venda,
                            ultima_hora_venda = EXCLUDED.ultima_hora_venda,
                            num_versao        = EXCLUDED.num_versao
                """, (comparacao_id, r["cod_farmacia"], r.get("nome_farmacia"), r.get("cnpj"), r.get("sit_contrato"), r.get("codigo_rede") or associacao, r.get("ultima_venda"), r.get("ultima_hora_venda"), r.get("num_versao")))

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

            # --- farmacias (consolidado + divergências) ---
            gold_by_cod = {str(r["cod_farmacia"]).strip(): r for r in resultados_gold_vendas}
            silver_by_cod = {str(r["cod_farmacia"]).strip(): r for r in resultados_silver_stgn_dedup}
            todas_farmacias = set(gold_by_cod.keys()) | set(silver_by_cod.keys())
            div_lookup = {str(d["cod_farmacia"]).strip(): d for d in divergencias}

            if todas_farmacias:
                cur.execute(
                    "DELETE FROM farmacias WHERE codigo_rede = %s AND cod_farmacia <> ALL(%s)",
                    (associacao, list(todas_farmacias)),
                )
            else:
                cur.execute("DELETE FROM farmacias WHERE codigo_rede = %s", (associacao,))

            for cod in todas_farmacias:
                r_gold = gold_by_cod.get(cod)
                r_silver = silver_by_cod.get(cod)
                div = div_lookup.get(cod, {})
                nome = (r_gold.get("nome_farmacia") if r_gold else None) or div.get("nome_farmacia")
                cnpj = (r_gold.get("cnpj") if r_gold else None) or div.get("cnpj")
                sit_contrato = (r_gold.get("sit_contrato") if r_gold else None) or div.get("sit_contrato")
                codigo_rede = (r_gold.get("codigo_rede") if r_gold else None) or div.get("codigo_rede") or associacao
                cur.execute("""
                    INSERT INTO farmacias
                        (comparacao_id, cod_farmacia, nome_farmacia, cnpj,
                         sit_contrato, codigo_rede,
                         ultima_venda_GoldVendas, ultima_hora_venda_GoldVendas,
                         ultima_venda_SilverSTGN_Dedup, ultima_hora_venda_SilverSTGN_Dedup,
                         tipo_divergencia, coletor_novo, coletor_bi_ultima_data, coletor_bi_ultima_hora,
                         num_versao)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, NULL, NULL, %s)
                    ON CONFLICT (codigo_rede, cod_farmacia) DO UPDATE
                        SET comparacao_id                      = EXCLUDED.comparacao_id,
                            nome_farmacia                      = EXCLUDED.nome_farmacia,
                            cnpj                               = EXCLUDED.cnpj,
                            sit_contrato                       = EXCLUDED.sit_contrato,
                            codigo_rede                        = EXCLUDED.codigo_rede,
                            ultima_venda_GoldVendas            = EXCLUDED.ultima_venda_GoldVendas,
                            ultima_hora_venda_GoldVendas       = EXCLUDED.ultima_hora_venda_GoldVendas,
                            ultima_venda_SilverSTGN_Dedup      = EXCLUDED.ultima_venda_SilverSTGN_Dedup,
                            ultima_hora_venda_SilverSTGN_Dedup = EXCLUDED.ultima_hora_venda_SilverSTGN_Dedup,
                            tipo_divergencia                   = EXCLUDED.tipo_divergencia,
                            coletor_novo                       = NULL,
                            coletor_bi_ultima_data             = NULL,
                            coletor_bi_ultima_hora             = NULL,
                            num_versao                         = EXCLUDED.num_versao
                """, (
                    comparacao_id, cod, nome, _sanitize_cnpj(cnpj),
                    sit_contrato, codigo_rede,
                    r_gold.get("ultima_venda") if r_gold else None,
                    r_gold.get("ultima_hora_venda") if r_gold else None,
                    r_silver.get("ultima_venda") if r_silver else None,
                    r_silver.get("ultima_hora_venda") if r_silver else None,
                    div.get("tipo_divergencia"),
                    div.get("num_versao") or (r_gold.get("num_versao") if r_gold else None),
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


def buscar_ultima_atualizacao_vendas_parceiros() -> Optional[str]:
    """Retorna a data/hora da atualização mais recente de vendas_parceiros."""
    with get_local_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(atualizado_em)::text FROM resultados_vendas_parceiros")
            row = cur.fetchone()
            return row[0] if row else None


def buscar_todos_consolidados() -> list[dict]:
    """Retorna todas as farmácias de todas as associações (dados da última rodada de cada uma)."""
    with get_local_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    c.associacao,
                    f.cod_farmacia,
                    f.nome_farmacia,
                    f.cnpj,
                    f.sit_contrato,
                    f.codigo_rede,
                    f.num_versao,
                    f.ultima_venda_GoldVendas::text,
                    f.ultima_hora_venda_GoldVendas::text,
                    f.ultima_venda_SilverSTGN_Dedup::text,
                    f.ultima_hora_venda_SilverSTGN_Dedup::text,
                    f.dat_hora_emissao_vendas_parceiros::text,
                    f.tipo_divergencia,
                    f.coletor_novo,
                    f.coletor_bi_ultima_data::text,
                    f.coletor_bi_ultima_hora::text,
                    f.classificacao,
                    c.executado_em::text AS atualizado_em
                FROM farmacias f
                LEFT JOIN comparacoes c ON c.id = f.comparacao_id
                ORDER BY f.codigo_rede, f.cod_farmacia
            """)
            return [dict(row) for row in cur.fetchall()]


def buscar_historico_por_associacao(associacao: str) -> list[dict]:
    """Busca o estado atual de todas as farmácias de uma associação."""
    with get_local_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    c.associacao,
                    f.cod_farmacia,
                    f.nome_farmacia,
                    f.cnpj,
                    f.sit_contrato,
                    f.codigo_rede,
                    f.num_versao,
                    f.ultima_venda_GoldVendas::text,
                    f.ultima_hora_venda_GoldVendas::text,
                    f.ultima_venda_SilverSTGN_Dedup::text,
                    f.ultima_hora_venda_SilverSTGN_Dedup::text,
                    f.dat_hora_emissao_vendas_parceiros::text,
                    f.tipo_divergencia,
                    f.coletor_novo,
                    f.coletor_bi_ultima_data::text,
                    f.coletor_bi_ultima_hora::text,
                    f.classificacao,
                    c.executado_em::text AS atualizado_em
                FROM farmacias f
                LEFT JOIN comparacoes c ON c.id = f.comparacao_id
                WHERE f.codigo_rede = %s
                ORDER BY f.cod_farmacia
            """, (associacao,))
            return [dict(row) for row in cur.fetchall()]


def salvar_status_farmacias(
    comparacao_id: int,
    associacao: str,
    status_farmacias: dict[str, str],
    coletor_bi: dict[str, str] | None = None,
    classificacao_dict: dict[str, str | None] | None = None,
) -> None:
    """Atualiza o status dos coletores (Business Connect + Coletor BI) em farmacias."""
    if not status_farmacias:
        return

    coletor_bi = coletor_bi or {}

    with get_local_connection() as conn:
        with conn.cursor() as cur:
            for cod_farmacia, coletor_novo in status_farmacias.items():
                dados_bi = coletor_bi.get(cod_farmacia) or {}
                cur.execute("""
                    UPDATE farmacias
                    SET coletor_novo           = %s,
                        coletor_bi_ultima_data = %s,
                        coletor_bi_ultima_hora = %s
                    WHERE codigo_rede = %s AND cod_farmacia = %s
                """, (
                    coletor_novo,
                    dados_bi.get("ultima_data"),
                    dados_bi.get("ultima_hora"),
                    associacao,
                    cod_farmacia,
                ))
                if classificacao_dict is not None:
                    cur.execute("""
                        UPDATE farmacias
                        SET classificacao = %s
                        WHERE codigo_rede = %s AND cod_farmacia = %s
                    """, (
                        classificacao_dict.get(cod_farmacia),
                        associacao,
                        cod_farmacia,
                    ))
        conn.commit()


def salvar_vendas_parceiros(resultados: list[dict]) -> int:
    """Persiste resultados de vendas_parceiros no PostgreSQL local via UPSERT."""
    with get_local_connection() as conn:
        with conn.cursor() as cur:
            # Limpa registros antigos que não estão mais no resultado
            novos_keys = {
                (str(r.get("associacao", "")).strip(), str(r["cod_farmacia"]).strip())
                for r in resultados
            }
            if novos_keys:
                cur.execute("DELETE FROM resultados_vendas_parceiros")

            for r in resultados:
                assoc = str(r.get("associacao", "")).strip()
                cur.execute("""
                    INSERT INTO resultados_vendas_parceiros
                        (associacao, cod_farmacia, nome_farmacia, sit_contrato,
                         farmacia, associacao_parceiro, ultima_venda_parceiros, atualizado_em)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (associacao, cod_farmacia) DO UPDATE
                        SET nome_farmacia        = EXCLUDED.nome_farmacia,
                            sit_contrato         = EXCLUDED.sit_contrato,
                            farmacia             = EXCLUDED.farmacia,
                            associacao_parceiro   = EXCLUDED.associacao_parceiro,
                            ultima_venda_parceiros = EXCLUDED.ultima_venda_parceiros,
                            atualizado_em        = CURRENT_TIMESTAMP
                """, (
                    assoc,
                    str(r["cod_farmacia"]).strip(),
                    r.get("nome_farmacia"),
                    r.get("sit_contrato"),
                    r.get("farmacia"),
                    r.get("associacao_parceiro"),
                    r.get("ultima_venda_parceiros"),
                ))
        conn.commit()
        return len(resultados)


def buscar_vendas_parceiros() -> list[dict]:
    """Retorna todos os resultados de vendas_parceiros persistidos."""
    with get_local_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    associacao,
                    cod_farmacia,
                    nome_farmacia,
                    sit_contrato,
                    farmacia,
                    associacao_parceiro,
                    ultima_venda_parceiros::text,
                    atualizado_em::text
                FROM resultados_vendas_parceiros
                ORDER BY ultima_venda_parceiros DESC
            """)
            return [dict(row) for row in cur.fetchall()]
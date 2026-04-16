"""Módulo de comparação entre resultados das duas queries do Redshift."""
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from app.queries import execute_gold_vendas, execute_silver_stgn_dedup, execute_cadfilia_por_codigos
from app.local_db import salvar_comparacao

logger = logging.getLogger(__name__)


@dataclass
class Divergencia:
    """Representa uma farmácia com divergência entre GoldVendas e SilverSTGN_Dedup.

    Tipos possíveis:
    - "data_diferente": presente em ambas as fontes mas com ultima_venda diferente
    - "apenas_gold_vendas": presente somente em associacao.vendas
    - "apenas_silver_stgn_dedup": presente somente em silver.cadcvend_staging_dedup
    """

    cod_farmacia: str
    nome_farmacia: Optional[str]
    cnpj: Optional[str]
    ultima_venda_GoldVendas: Optional[str]
    ultima_hora_venda_GoldVendas: Optional[str]
    ultima_venda_SilverSTGN_Dedup: Optional[str]
    ultima_hora_venda_SilverSTGN_Dedup: Optional[str]
    tipo_divergencia: str  # "data_diferente", "apenas_gold_vendas", "apenas_silver_stgn_dedup"


@dataclass
class ResultadoComparacao:
    """Resultado completo de uma comparação entre GoldVendas e SilverSTGN_Dedup.

    Contém os totais de registros em cada fonte, o número de divergências
    e a lista detalhada de cada divergência encontrada.
    """

    associacao: str
    total_gold_vendas: int
    total_silver_stgn_dedup: int
    total_divergencias: int
    divergencias: list[Divergencia] = field(default_factory=list)
    comparacao_id: Optional[int] = None  # ID no banco local após salvar
    todas_farmacias: set = field(default_factory=set)  # union de GoldVendas + SilverSTGN_Dedup


def comparar_resultados(associacao: str, salvar: bool = True) -> ResultadoComparacao:
    """Executa as queries no Redshift e compara os resultados por cod_farmacia.

    Busca a última venda registrada por farmácia em cada fonte (sem filtro de data)
    e identifica 3 tipos de divergência em ultima_venda:
    - data_diferente: farmácia presente em ambas mas com datas distintas
    - apenas_gold_vendas: farmácia presente somente em associacao.vendas
    - apenas_silver_stgn_dedup: farmácia presente somente em silver.cadcvend_staging_dedup

    Args:
        associacao: Código da associação para filtrar
        salvar: Se True, persiste todos os resultados no PostgreSQL local

    Returns:
        ResultadoComparacao com totais e lista completa de divergências
    """
    logger.info("🔍 Iniciando comparação — associacao=%s", associacao)
    t_total = time.perf_counter()

    resultados_gold_vendas = execute_gold_vendas(associacao)
    resultados_silver_stgn_dedup = execute_silver_stgn_dedup(associacao)

    gold_by_farmacia = {str(r["cod_farmacia"]).strip(): r for r in resultados_gold_vendas}
    silver_by_farmacia = {str(r["cod_farmacia"]).strip(): r for r in resultados_silver_stgn_dedup}

    todas_farmacias = set(gold_by_farmacia.keys()) | set(silver_by_farmacia.keys())

    # Busca nome/CNPJ no cadastro (cadfilia) apenas para farmácias sem registro em Gold
    silver_only_codes = [cod for cod in silver_by_farmacia if cod not in gold_by_farmacia]
    cadfilia_lookup = execute_cadfilia_por_codigos(silver_only_codes)
    logger.info(
        "cadfilia lookup — silver_only_codes=%d, encontrados=%d | exemplos codes: %s",
        len(silver_only_codes),
        len(cadfilia_lookup),
        silver_only_codes[:5],
    )

    divergencias = []

    for cod in todas_farmacias:
        r_gold = gold_by_farmacia.get(cod)
        r_silver = silver_by_farmacia.get(cod)

        if r_gold and not r_silver:
            divergencias.append(Divergencia(
                cod_farmacia=cod,
                nome_farmacia=r_gold.get("nome_farmacia"),
                cnpj=r_gold.get("cnpj"),
                ultima_venda_GoldVendas=str(r_gold.get("ultima_venda")) if r_gold.get("ultima_venda") else None,
                ultima_hora_venda_GoldVendas=str(r_gold.get("ultima_hora_venda")) if r_gold.get("ultima_hora_venda") else None,
                ultima_venda_SilverSTGN_Dedup=None,
                ultima_hora_venda_SilverSTGN_Dedup=None,
                tipo_divergencia="apenas_gold_vendas"
            ))
        elif r_silver and not r_gold:
            cadfilia = cadfilia_lookup.get(cod, {})
            divergencias.append(Divergencia(
                cod_farmacia=cod,
                nome_farmacia=cadfilia.get("nome_farmacia"),
                cnpj=cadfilia.get("cnpj"),
                ultima_venda_GoldVendas=None,
                ultima_hora_venda_GoldVendas=None,
                ultima_venda_SilverSTGN_Dedup=str(r_silver.get("ultima_venda")) if r_silver.get("ultima_venda") else None,
                ultima_hora_venda_SilverSTGN_Dedup=str(r_silver.get("ultima_hora_venda")) if r_silver.get("ultima_hora_venda") else None,
                tipo_divergencia="apenas_silver_stgn_dedup"
            ))
        else:
            venda_gold = str(r_gold.get("ultima_venda")) if r_gold.get("ultima_venda") else None
            venda_silver = str(r_silver.get("ultima_venda")) if r_silver.get("ultima_venda") else None

            if venda_gold != venda_silver:
                divergencias.append(Divergencia(
                    cod_farmacia=cod,
                    nome_farmacia=r_gold.get("nome_farmacia"),
                    cnpj=r_gold.get("cnpj"),
                    ultima_venda_GoldVendas=venda_gold,
                    ultima_hora_venda_GoldVendas=str(r_gold.get("ultima_hora_venda")) if r_gold.get("ultima_hora_venda") else None,
                    ultima_venda_SilverSTGN_Dedup=venda_silver,
                    ultima_hora_venda_SilverSTGN_Dedup=str(r_silver.get("ultima_hora_venda")) if r_silver.get("ultima_hora_venda") else None,
                    tipo_divergencia="data_diferente"
                ))

    resultado = ResultadoComparacao(
        associacao=associacao,
        total_gold_vendas=len(resultados_gold_vendas),
        total_silver_stgn_dedup=len(resultados_silver_stgn_dedup),
        total_divergencias=len(divergencias),
        divergencias=divergencias,
        todas_farmacias=todas_farmacias,
    )

    if salvar:
        divergencias_dict = [
            {
                "cod_farmacia": d.cod_farmacia,
                "nome_farmacia": d.nome_farmacia,
                "cnpj": d.cnpj,
                "ultima_venda_GoldVendas": d.ultima_venda_GoldVendas,
                "ultima_hora_venda_GoldVendas": d.ultima_hora_venda_GoldVendas,
                "ultima_venda_SilverSTGN_Dedup": d.ultima_venda_SilverSTGN_Dedup,
                "ultima_hora_venda_SilverSTGN_Dedup": d.ultima_hora_venda_SilverSTGN_Dedup,
                "tipo_divergencia": d.tipo_divergencia
            }
            for d in divergencias
        ]
        resultado.comparacao_id = salvar_comparacao(
            associacao, resultados_gold_vendas, resultados_silver_stgn_dedup, divergencias_dict
        )

    elapsed_total = time.perf_counter() - t_total
    logger.info(
        "🏁 Comparação concluída em %.2fs — gold=%d, silver=%d, divergências=%d",
        elapsed_total,
        resultado.total_gold_vendas,
        resultado.total_silver_stgn_dedup,
        resultado.total_divergencias,
    )
    return resultado

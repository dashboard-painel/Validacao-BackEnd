"""Módulo de comparação entre resultados das duas queries do Redshift."""
from dataclasses import dataclass, field
from typing import Optional

from app.queries import execute_gold_vendas, execute_silver_stgn_dedup, buscar_nomes_farmacias
from app.local_db import salvar_comparacao


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
    dat_emissao_filtro: str
    total_gold_vendas: int
    total_silver_stgn_dedup: int
    total_divergencias: int
    divergencias: list[Divergencia] = field(default_factory=list)
    comparacao_id: Optional[int] = None  # ID no banco local após salvar
    todas_farmacias: set = field(default_factory=set)  # union de GoldVendas + SilverSTGN_Dedup


def comparar_resultados(associacao: str, dat_emissao: str, salvar: bool = True) -> ResultadoComparacao:
    """Executa as queries no Redshift e compara os resultados por cod_farmacia.

    Identifica 3 tipos de divergência em ultima_venda:
    - data_diferente: farmácia presente em ambas mas com datas distintas
    - apenas_gold_vendas: farmácia presente somente em associacao.vendas
    - apenas_silver_stgn_dedup: farmácia presente somente em silver.cadcvend_staging_dedup

    Args:
        associacao: Código da associação para filtrar
        dat_emissao: Data de emissão mínima no formato YYYY-MM-DD
        salvar: Se True, persiste todos os resultados no PostgreSQL local

    Returns:
        ResultadoComparacao com totais e lista completa de divergências
    """
    resultados_gold_vendas = execute_gold_vendas(associacao, dat_emissao)
    resultados_silver_stgn_dedup = execute_silver_stgn_dedup(associacao, dat_emissao)
    nomes_lookup = buscar_nomes_farmacias(associacao)

    gold_by_farmacia = {str(r["cod_farmacia"]): r for r in resultados_gold_vendas}
    silver_by_farmacia = {str(r["cod_farmacia"]): r for r in resultados_silver_stgn_dedup}

    todas_farmacias = set(gold_by_farmacia.keys()) | set(silver_by_farmacia.keys())

    divergencias = []

    for cod in todas_farmacias:
        r_gold = gold_by_farmacia.get(cod)
        r_silver = silver_by_farmacia.get(cod)

        if r_gold and not r_silver:
            divergencias.append(Divergencia(
                cod_farmacia=cod,
                nome_farmacia=r_gold.get("nome_farmacia"),
                ultima_venda_GoldVendas=str(r_gold.get("ultima_venda")) if r_gold.get("ultima_venda") else None,
                ultima_hora_venda_GoldVendas=str(r_gold.get("ultima_hora_venda")) if r_gold.get("ultima_hora_venda") else None,
                ultima_venda_SilverSTGN_Dedup=None,
                ultima_hora_venda_SilverSTGN_Dedup=None,
                tipo_divergencia="apenas_gold_vendas"
            ))
        elif r_silver and not r_gold:
            divergencias.append(Divergencia(
                cod_farmacia=cod,
                nome_farmacia=nomes_lookup.get(cod),
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
                    ultima_venda_GoldVendas=venda_gold,
                    ultima_hora_venda_GoldVendas=str(r_gold.get("ultima_hora_venda")) if r_gold.get("ultima_hora_venda") else None,
                    ultima_venda_SilverSTGN_Dedup=venda_silver,
                    ultima_hora_venda_SilverSTGN_Dedup=str(r_silver.get("ultima_hora_venda")) if r_silver.get("ultima_hora_venda") else None,
                    tipo_divergencia="data_diferente"
                ))

    resultado = ResultadoComparacao(
        associacao=associacao,
        dat_emissao_filtro=dat_emissao,
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
                "ultima_venda_GoldVendas": d.ultima_venda_GoldVendas,
                "ultima_hora_venda_GoldVendas": d.ultima_hora_venda_GoldVendas,
                "ultima_venda_SilverSTGN_Dedup": d.ultima_venda_SilverSTGN_Dedup,
                "ultima_hora_venda_SilverSTGN_Dedup": d.ultima_hora_venda_SilverSTGN_Dedup,
                "tipo_divergencia": d.tipo_divergencia
            }
            for d in divergencias
        ]
        resultado.comparacao_id = salvar_comparacao(
            associacao, dat_emissao, resultados_gold_vendas, resultados_silver_stgn_dedup, divergencias_dict, nomes_lookup
        )

    return resultado

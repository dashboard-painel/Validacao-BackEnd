"""Módulo de comparação entre resultados das duas queries do Redshift."""
from dataclasses import dataclass, field
from typing import Optional

from app.queries import execute_query_1, execute_query_2
from app.local_db import salvar_comparacao


@dataclass
class Divergencia:
    """Representa uma farmácia com divergência entre Q1 e Q2.

    Tipos possíveis:
    - "data_diferente": presente em ambas as queries mas com ultima_venda diferente
    - "apenas_q1": presente somente na Query 1 (associacao.vendas)
    - "apenas_q2": presente somente na Query 2 (silver.cadcvend_staging_dedup)
    """

    cod_farmacia: str
    nome_farmacia: Optional[str]
    ultima_venda_GoldVendas: Optional[str]
    ultima_hora_venda_GoldVendas: Optional[str]
    ultima_venda_SilverSTGN_Dedup: Optional[str]
    ultima_hora_venda_SilverSTGN_Dedup: Optional[str]
    tipo_divergencia: str  # "data_diferente", "apenas_q1", "apenas_q2"


@dataclass
class ResultadoComparacao:
    """Resultado completo de uma comparação entre Q1 e Q2.

    Contém os totais de registros em cada query, o número de divergências
    e a lista detalhada de cada divergência encontrada.
    """

    associacao: str
    dat_emissao_filtro: str
    total_q1: int
    total_q2: int
    total_divergencias: int
    divergencias: list[Divergencia] = field(default_factory=list)
    comparacao_id: Optional[int] = None  # ID no banco local após salvar
    todas_farmacias: set = field(default_factory=set)  # union de q1 + q2


def comparar_resultados(associacao: str, dat_emissao: str, salvar: bool = True) -> ResultadoComparacao:
    """Executa as 2 queries no Redshift e compara os resultados por cod_farmacia.

    Identifica 3 tipos de divergência em ultima_venda:
    - data_diferente: farmácia presente em ambas mas com datas distintas
    - apenas_q1: farmácia presente somente em associacao.vendas
    - apenas_q2: farmácia presente somente em silver.cadcvend_staging_dedup

    Args:
        associacao: Código da associação para filtrar
        dat_emissao: Data de emissão mínima no formato YYYY-MM-DD
        salvar: Se True, persiste todos os resultados no PostgreSQL local

    Returns:
        ResultadoComparacao com totais e lista completa de divergências
    """
    resultados_q1 = execute_query_1(associacao, dat_emissao)
    resultados_q2 = execute_query_2(associacao, dat_emissao)

    q1_by_farmacia = {str(r["cod_farmacia"]): r for r in resultados_q1}
    q2_by_farmacia = {str(r["cod_farmacia"]): r for r in resultados_q2}

    todas_farmacias = set(q1_by_farmacia.keys()) | set(q2_by_farmacia.keys())

    divergencias = []

    for cod in todas_farmacias:
        r1 = q1_by_farmacia.get(cod)
        r2 = q2_by_farmacia.get(cod)

        if r1 and not r2:
            divergencias.append(Divergencia(
                cod_farmacia=cod,
                nome_farmacia=r1.get("nome_farmacia"),
                ultima_venda_GoldVendas=str(r1.get("ultima_venda")) if r1.get("ultima_venda") else None,
                ultima_hora_venda_GoldVendas=str(r1.get("ultima_hora_venda")) if r1.get("ultima_hora_venda") else None,
                ultima_venda_SilverSTGN_Dedup=None,
                ultima_hora_venda_SilverSTGN_Dedup=None,
                tipo_divergencia="apenas_q1"
            ))
        elif r2 and not r1:
            divergencias.append(Divergencia(
                cod_farmacia=cod,
                nome_farmacia=None,
                ultima_venda_GoldVendas=None,
                ultima_hora_venda_GoldVendas=None,
                ultima_venda_SilverSTGN_Dedup=str(r2.get("ultima_venda")) if r2.get("ultima_venda") else None,
                ultima_hora_venda_SilverSTGN_Dedup=str(r2.get("ultima_hora_venda")) if r2.get("ultima_hora_venda") else None,
                tipo_divergencia="apenas_q2"
            ))
        else:
            venda_q1 = str(r1.get("ultima_venda")) if r1.get("ultima_venda") else None
            venda_q2 = str(r2.get("ultima_venda")) if r2.get("ultima_venda") else None

            if venda_q1 != venda_q2:
                divergencias.append(Divergencia(
                    cod_farmacia=cod,
                    nome_farmacia=r1.get("nome_farmacia"),
                    ultima_venda_GoldVendas=venda_q1,
                    ultima_hora_venda_GoldVendas=str(r1.get("ultima_hora_venda")) if r1.get("ultima_hora_venda") else None,
                    ultima_venda_SilverSTGN_Dedup=venda_q2,
                    ultima_hora_venda_SilverSTGN_Dedup=str(r2.get("ultima_hora_venda")) if r2.get("ultima_hora_venda") else None,
                    tipo_divergencia="data_diferente"
                ))

    resultado = ResultadoComparacao(
        associacao=associacao,
        dat_emissao_filtro=dat_emissao,
        total_q1=len(resultados_q1),
        total_q2=len(resultados_q2),
        total_divergencias=len(divergencias),
        divergencias=divergencias,
        todas_farmacias=todas_farmacias,  # set já calculado na linha 67
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
            associacao, dat_emissao, resultados_q1, resultados_q2, divergencias_dict
        )

    return resultado

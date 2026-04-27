"""Modelos de domínio internos da comparação (não são schemas de API)."""
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Divergencia:

    cod_farmacia: str
    nome_farmacia: Optional[str]
    cnpj: Optional[str]
    ultima_venda_GoldVendas: Optional[str]
    ultima_hora_venda_GoldVendas: Optional[str]
    # ultima_venda_SilverSTGN_Dedup: Optional[str]  # silver desativado
    # ultima_hora_venda_SilverSTGN_Dedup: Optional[str]  # silver desativado
    tipo_divergencia: str  # "data_diferente", "apenas_gold_vendas", "apenas_silver_stgn_dedup"
    sit_contrato: Optional[str] = None
    codigo_rede: Optional[str] = None
    num_versao: Optional[str] = None


@dataclass
class ResultadoComparacao:

    associacao: str
    total_gold_vendas: int
    # total_silver_stgn_dedup: int  # silver desativado
    total_divergencias: int
    divergencias: list[Divergencia] = field(default_factory=list)
    comparacao_id: Optional[int] = None
    todas_farmacias: set = field(default_factory=set)
    resultados_gold_vendas: list[dict] = field(default_factory=list)
    # resultados_silver_stgn_dedup: list[dict] = field(default_factory=list)  # silver desativado


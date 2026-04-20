"""Modelos de domínio internos da comparação (não são schemas de API)."""
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Divergencia:
    """Farmácia com divergência entre GoldVendas e SilverSTGN_Dedup.

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
    comparacao_id: Optional[int] = None
    todas_farmacias: set = field(default_factory=set)
    resultados_gold_vendas: list[dict] = field(default_factory=list)
    resultados_silver_stgn_dedup: list[dict] = field(default_factory=list)

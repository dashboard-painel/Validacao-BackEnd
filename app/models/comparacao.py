"""Modelos de domínio internos da comparação (não são schemas de API)."""
from dataclasses import asdict, dataclass, field
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

    @classmethod
    def from_gold_silver(
        cls,
        cod: str,
        r_gold: dict | None,
        r_silver: dict | None,
        tipo: str,
        cadastro_extra: dict | None = None,
    ) -> "Divergencia":
        """Cria Divergencia a partir dos dicts do Redshift.

        Args:
            cod: Código da farmácia
            r_gold: Dict do resultado gold (ou None)
            r_silver: Dict do resultado silver (ou None)
            tipo: Tipo de divergência
            cadastro_extra: Dict com dados cadastrais extras (cadfilia lookup)
        """
        extra = cadastro_extra or {}
        nome = (r_gold.get("nome_farmacia") if r_gold else None) or extra.get("nome_farmacia")
        cnpj = (r_gold.get("cnpj") if r_gold else None) or extra.get("cnpj")
        sit = (r_gold.get("sit_contrato") if r_gold else None) or extra.get("sit_contrato")
        rede = (r_gold.get("codigo_rede") if r_gold else None) or extra.get("codigo_rede")
        versao = r_gold.get("num_versao") if r_gold else None

        def _str(val) -> str | None:
            return str(val) if val else None

        return cls(
            cod_farmacia=cod,
            nome_farmacia=nome,
            cnpj=cnpj,
            sit_contrato=sit,
            codigo_rede=rede,
            num_versao=versao,
            ultima_venda_GoldVendas=_str(r_gold.get("ultima_venda")) if r_gold else None,
            ultima_hora_venda_GoldVendas=_str(r_gold.get("ultima_hora_venda")) if r_gold else None,
            ultima_venda_SilverSTGN_Dedup=_str(r_silver.get("ultima_venda")) if r_silver else None,
            ultima_hora_venda_SilverSTGN_Dedup=_str(r_silver.get("ultima_hora_venda")) if r_silver else None,
            tipo_divergencia=tipo,
        )

    def to_dict(self) -> dict:
        """Converte para dict (usado na persistência local)."""
        return asdict(self)


@dataclass
class ResultadoComparacao:

    associacao: str
    total_gold_vendas: int
    # total_silver_stgn_dedup: int  # silver desativado
    total_divergencias: int
    divergencias: list[Divergencia] = field(default_factory=list)
    comparacao_id: Optional[int] = None
    todas_farmacias: set[str] = field(default_factory=set)
    resultados_gold_vendas: list[dict] = field(default_factory=list)
    # resultados_silver_stgn_dedup: list[dict] = field(default_factory=list)  # silver desativado


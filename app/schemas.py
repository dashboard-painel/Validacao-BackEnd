"""Schemas Pydantic para validação e serialização de respostas da API."""
from typing import Optional

from pydantic import BaseModel, Field


class ComparacaoRequest(BaseModel):
    """Parâmetros para execução da comparação via POST."""

    associacao: str = Field(..., description="Código da associação para filtrar")

    model_config = {
        "json_schema_extra": {
            "example": {
                "associacao": "80",
            }
        }
    }


class DivergenciaResponse(BaseModel):
    """Representa uma farmácia com divergência entre GoldVendas e SilverSTGN_Dedup.

    Tipos de divergência:
    - data_diferente: presente em ambas mas com ultima_venda diferente
    - apenas_gold_vendas: presente somente em associacao.vendas
    - apenas_silver_stgn_dedup: presente somente em silver.cadcvend_staging_dedup
    """

    cod_farmacia: str = Field(..., description="Código da farmácia")
    nome_farmacia: Optional[str] = Field(None, description="Nome da farmácia (quando disponível)")
    cnpj: Optional[str] = Field(None, description="CNPJ da farmácia (quando disponível)")
    ultima_venda_GoldVendas: Optional[str] = Field(None, description="Data da última venda em associacao.vendas (YYYY-MM-DD)")
    ultima_hora_venda_GoldVendas: Optional[str] = Field(None, description="Hora da última venda em associacao.vendas")
    ultima_venda_SilverSTGN_Dedup: Optional[str] = Field(None, description="Data da última venda em silver.cadcvend_staging_dedup (YYYY-MM-DD)")
    ultima_hora_venda_SilverSTGN_Dedup: Optional[str] = Field(None, description="Hora da última venda em silver.cadcvend_staging_dedup")
    tipo_divergencia: str = Field(..., description="Tipo: data_diferente, apenas_gold_vendas, apenas_silver_stgn_dedup")

    model_config = {
        "json_schema_extra": {
            "example": {
                "cod_farmacia": "001",
                "nome_farmacia": "Farmacia Central",
                "cnpj": "12.345.678/0001-99",
                "ultima_venda_GoldVendas": "2024-03-15",
                "ultima_hora_venda_GoldVendas": "14:30:00",
                "ultima_venda_SilverSTGN_Dedup": "2024-03-14",
                "ultima_hora_venda_SilverSTGN_Dedup": "09:00:00",
                "tipo_divergencia": "data_diferente",
            }
        }
    }


class FarmaciaStatusResponse(BaseModel):
    """Status de migração de uma farmácia no Business Connect."""

    cod_farmacia: str = Field(..., description="Código da farmácia")
    coletor_novo: str = Field(
        ...,
        description=(
            "Status do coletor: "
            "'OK, sem registro' | "
            "'Pendente de envio no dia YYYY-MM-DD' | "
            "'Indisponível'"
        ),
    )


class ResultadoConsolidadoResponse(BaseModel):
    """Registro consolidado com dados de GoldVendas (associacao.vendas) e SilverSTGN_Dedup lado a lado."""

    cod_farmacia: str = Field(..., description="Código da farmácia")
    nome_farmacia: Optional[str] = Field(None, description="Nome da farmácia (disponível em associacao.vendas)")
    cnpj: Optional[str] = Field(None, description="CNPJ da farmácia (disponível em associacao.vendas)")
    ultima_venda_GoldVendas: Optional[str] = Field(None, description="Última venda em associacao.vendas")
    ultima_hora_venda_GoldVendas: Optional[str] = Field(None, description="Hora da última venda em associacao.vendas")
    ultima_venda_SilverSTGN_Dedup: Optional[str] = Field(None, description="Última venda em silver.cadcvend_staging_dedup")
    ultima_hora_venda_SilverSTGN_Dedup: Optional[str] = Field(None, description="Hora da última venda em silver.cadcvend_staging_dedup")


class ComparacaoResponse(BaseModel):
    """Resultado completo de uma comparação entre GoldVendas e SilverSTGN_Dedup."""

    associacao: str = Field(..., description="Código da associação comparada")
    total_gold_vendas: int = Field(..., ge=0, description="Total de registros em associacao.vendas")
    total_silver_stgn_dedup: int = Field(..., ge=0, description="Total de registros em silver.cadcvend_staging_dedup")
    total_divergencias: int = Field(..., ge=0, description="Quantidade de divergências encontradas")
    comparacao_id: Optional[int] = Field(None, description="ID da comparação salva no banco local")
    divergencias: list[DivergenciaResponse] = Field(
        default_factory=list,
        description="Lista detalhada das divergências",
    )
    status_farmacias: list[FarmaciaStatusResponse] = Field(
        default_factory=list,
        description="Status de migração de todas as farmácias (GoldVendas + SilverSTGN_Dedup) no Business Connect",
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "associacao": "123",
                "total_gold_vendas": 150,
                "total_silver_stgn_dedup": 148,
                "total_divergencias": 5,
                "comparacao_id": 42,
                "divergencias": [],
            }
        }
    }

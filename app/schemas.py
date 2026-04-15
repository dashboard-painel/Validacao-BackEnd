"""Schemas Pydantic para validação e serialização de respostas da API."""
from datetime import datetime, timedelta
from typing import Optional

from pydantic import BaseModel, Field


def _default_dat_emissao() -> str:
    return (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")


class ComparacaoRequest(BaseModel):
    """Parâmetros para execução da comparação via POST."""

    associacao: str = Field(..., description="Código da associação para filtrar")
    dat_emissao: Optional[str] = Field(
        default=None,
        description="Data mínima de emissão (YYYY-MM-DD). Default: 30 dias atrás",
        pattern=r"^\d{4}-\d{2}-\d{2}$",
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "associacao": "80",
                "dat_emissao": "2026-04-01",
            }
        }
    }


class DivergenciaResponse(BaseModel):
    """Representa uma farmácia com divergência entre Q1 e Q2.

    Tipos de divergência:
    - data_diferente: presente em ambas mas com ultima_venda diferente
    - apenas_q1: presente somente na Query 1
    - apenas_q2: presente somente na Query 2
    """

    cod_farmacia: str = Field(..., description="Código da farmácia")
    nome_farmacia: Optional[str] = Field(None, description="Nome da farmácia (quando disponível)")
    ultima_venda_GoldVendas: Optional[str] = Field(None, description="Data da última venda na Q1 - associacao.vendas (YYYY-MM-DD)")
    ultima_hora_venda_GoldVendas: Optional[str] = Field(None, description="Hora da última venda na Q1 - associacao.vendas")
    ultima_venda_SilverSTGN_Dedup: Optional[str] = Field(None, description="Data da última venda na Q2 - silver.cadcvend_staging_dedup (YYYY-MM-DD)")
    ultima_hora_venda_SilverSTGN_Dedup: Optional[str] = Field(None, description="Hora da última venda na Q2 - silver.cadcvend_staging_dedup")
    tipo_divergencia: str = Field(..., description="Tipo: data_diferente, apenas_q1, apenas_q2")

    model_config = {
        "json_schema_extra": {
            "example": {
                "cod_farmacia": "001",
                "nome_farmacia": "Farmacia Central",
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


class ComparacaoResponse(BaseModel):
    """Resultado completo de uma comparação entre Q1 e Q2."""

    associacao: str = Field(..., description="Código da associação comparada")
    dat_emissao_filtro: str = Field(..., description="Data de emissão usada como filtro (YYYY-MM-DD)")
    total_q1: int = Field(..., ge=0, description="Total de registros na Query 1")
    total_q2: int = Field(..., ge=0, description="Total de registros na Query 2")
    total_divergencias: int = Field(..., ge=0, description="Quantidade de divergências encontradas")
    comparacao_id: Optional[int] = Field(None, description="ID da comparação salva no banco local")
    divergencias: list[DivergenciaResponse] = Field(
        default_factory=list,
        description="Lista detalhada das divergências",
    )
    status_farmacias: list[FarmaciaStatusResponse] = Field(
        default_factory=list,
        description="Status de migração de todas as farmácias (q1 + q2) no Business Connect",
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "associacao": "123",
                "dat_emissao_filtro": "2024-01-01",
                "total_q1": 150,
                "total_q2": 148,
                "total_divergencias": 5,
                "comparacao_id": 42,
                "divergencias": [],
            }
        }
    }

from typing import Optional

from pydantic import BaseModel, Field


class VendasParceirosItemResponse(BaseModel):
    cod_farmacia: str = Field(..., description="Código da farmácia (dimensao_cadastro_lojas)")
    nome_farmacia: Optional[str] = Field(None, description="Nome da farmácia")
    sit_contrato: Optional[str] = Field(None, description="Situação do contrato")
    associacao: str = Field(..., description="Código da rede (codigo_rede da dimensao)")
    farmacia: Optional[str] = Field(None, description="Identificador da farmácia em vendas_parceiros")
    associacao_parceiro: Optional[str] = Field(None, description="Associação em vendas_parceiros")
    ultima_venda_parceiros: Optional[str] = Field(None, description="Data/hora da última venda em vendas_parceiros")


class VendasParceirosResponse(BaseModel):
    total: int = Field(..., ge=0, description="Total de registros retornados")
    resultados: list[VendasParceirosItemResponse] = Field(default_factory=list, description="Lista de registros")

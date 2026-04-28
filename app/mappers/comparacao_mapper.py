from app.models.comparacao import Divergencia, ResultadoComparacao
from app.schemas import (
    ComparacaoResponse,
    DivergenciaResponse,
    FarmaciaStatusResponse,
    ResultadoConsolidadoResponse,
    VendasParceirosItemResponse,
)

from app.utils import camadas_atrasadas


def montar_divergencia_response(
    d: Divergencia,
    lista_camadas_atrasadas: list[str] | None,
    camadas_sem_dados: list[str] | None,
    classificacao: str | None = None,
) -> DivergenciaResponse:
    return DivergenciaResponse(
        cod_farmacia=d.cod_farmacia,
        nome_farmacia=d.nome_farmacia,
        cnpj=d.cnpj,
        sit_contrato=d.sit_contrato,
        codigo_rede=d.codigo_rede,
        num_versao=d.num_versao,
        ultima_venda_GoldVendas=d.ultima_venda_GoldVendas,
        ultima_hora_venda_GoldVendas=d.ultima_hora_venda_GoldVendas,
        ultima_venda_SilverSTGN_Dedup=d.ultima_venda_SilverSTGN_Dedup,
        ultima_hora_venda_SilverSTGN_Dedup=d.ultima_hora_venda_SilverSTGN_Dedup,
        tipo_divergencia=d.tipo_divergencia,
        camadas_atrasadas=lista_camadas_atrasadas,
        camadas_sem_dados=camadas_sem_dados,
        classificacao=classificacao,
    )


def montar_status_farmacia_response(cod: str, status: str, coletor_bi: dict | None = None) -> FarmaciaStatusResponse:
    dados_bi = coletor_bi or {}
    return FarmaciaStatusResponse(
        cod_farmacia=cod,
        coletor_novo=status,
        coletor_bi_ultima_data=dados_bi.get("ultima_data"),
        coletor_bi_ultima_hora=dados_bi.get("ultima_hora"),
    )


def montar_comparacao_response(
    resultado: ResultadoComparacao,
    divergencias: list[DivergenciaResponse],
    status_farmacias: list[FarmaciaStatusResponse],
) -> ComparacaoResponse:
    return ComparacaoResponse(
        associacao=resultado.associacao,
        total_gold_vendas=resultado.total_gold_vendas,
        total_silver_stgn_dedup=resultado.total_silver_stgn_dedup,
        total_divergencias=resultado.total_divergencias,
        comparacao_id=resultado.comparacao_id,
        divergencias=divergencias,
        status_farmacias=status_farmacias,
    )


def montar_vendas_parceiros_item(row: dict) -> VendasParceirosItemResponse:
    """Converte um dict de vendas_parceiros para VendasParceirosItemResponse."""
    return VendasParceirosItemResponse(
        cod_farmacia=str(row["cod_farmacia"]).strip(),
        nome_farmacia=row.get("nome_farmacia"),
        sit_contrato=row.get("sit_contrato"),
        associacao=str(row.get("associacao", "")).strip(),
        farmacia=str(row["farmacia"]) if row.get("farmacia") is not None else None,
        associacao_parceiro=str(row["associacao_parceiro"]) if row.get("associacao_parceiro") else None,
        ultima_venda_parceiros=str(row["ultima_venda_parceiros"]) if row.get("ultima_venda_parceiros") else None,
    )


def montar_resultado_consolidado(row: dict) -> ResultadoConsolidadoResponse:
    c_atrasadas, c_sem_dados = camadas_atrasadas(
        row.get("ultima_venda_goldvendas"),
        row.get("ultima_venda_silverstgn_dedup"),
        row.get("coletor_novo"),
    )

    return ResultadoConsolidadoResponse(
        associacao=row.get("associacao"),
        cod_farmacia=row["cod_farmacia"],
        nome_farmacia=row.get("nome_farmacia"),
        cnpj=row.get("cnpj"),
        sit_contrato=row.get("sit_contrato"),
        codigo_rede=row.get("codigo_rede"),
        num_versao=row.get("num_versao"),
        ultima_venda_GoldVendas=row.get("ultima_venda_goldvendas"),
        ultima_hora_venda_GoldVendas=row.get("ultima_hora_venda_goldvendas"),
        ultima_venda_SilverSTGN_Dedup=row.get("ultima_venda_silverstgn_dedup"),
        ultima_hora_venda_SilverSTGN_Dedup=row.get("ultima_hora_venda_silverstgn_dedup"),
        coletor_novo=row.get("coletor_novo"),
        coletor_bi_ultima_data=row.get("coletor_bi_ultima_data"),
        coletor_bi_ultima_hora=row.get("coletor_bi_ultima_hora"),
        tipo_divergencia=row.get("tipo_divergencia"),
        atualizado_em=row.get("atualizado_em"),
        camadas_atrasadas=c_atrasadas,
        camadas_sem_dados=c_sem_dados,
        classificacao=row.get("classificacao"),
    )
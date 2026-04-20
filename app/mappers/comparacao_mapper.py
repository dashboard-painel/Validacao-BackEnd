from app.schemas import (
    ComparacaoResponse,
    DivergenciaResponse,
    FarmaciaStatusResponse,
    ResultadoConsolidadoResponse,
)

from app.utils import camadas_atrasadas


def montar_divergencia_response(d, camadas_atrasadas, camadas_sem_dados) -> DivergenciaResponse:
    return DivergenciaResponse(
        cod_farmacia=d.cod_farmacia,
        nome_farmacia=d.nome_farmacia,
        cnpj=d.cnpj,
        ultima_venda_GoldVendas=d.ultima_venda_GoldVendas,
        ultima_hora_venda_GoldVendas=d.ultima_hora_venda_GoldVendas,
        ultima_venda_SilverSTGN_Dedup=d.ultima_venda_SilverSTGN_Dedup,
        ultima_hora_venda_SilverSTGN_Dedup=d.ultima_hora_venda_SilverSTGN_Dedup,
        tipo_divergencia=d.tipo_divergencia,
        camadas_atrasadas=camadas_atrasadas,
        camadas_sem_dados=camadas_sem_dados,
    )


def montar_status_farmacia_response(cod: str, status: str, coletor_bi: dict | None = None) -> FarmaciaStatusResponse:
    dados_bi = coletor_bi or {}
    return FarmaciaStatusResponse(
        cod_farmacia=cod,
        coletor_novo=status,
        coletor_bi_ultima_data=dados_bi.get("ultima_data"),
        coletor_bi_ultima_hora=dados_bi.get("ultima_hora"),
    )


def montar_comparacao_response(resultado, divergencias, status_farmacias) -> ComparacaoResponse:
    return ComparacaoResponse(
        associacao=resultado.associacao,
        total_gold_vendas=resultado.total_gold_vendas,
        total_silver_stgn_dedup=resultado.total_silver_stgn_dedup,
        total_divergencias=resultado.total_divergencias,
        comparacao_id=resultado.comparacao_id,
        divergencias=divergencias,
        status_farmacias=status_farmacias,
    )

def montar_resultado_consolidado(row) -> ResultadoConsolidadoResponse:
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
    )
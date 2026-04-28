import logging
import time
from concurrent.futures import ThreadPoolExecutor

from app.models.comparacao import (
    Divergencia,
    ResultadoComparacao,
    ComparacaoResponse,
    DivergenciaResponse,
    FarmaciaStatusResponse,
    ResultadoConsolidadoResponse,
)
from app.repositories.comparacao_repository import (
    buscar_status_farmacias,
    buscar_classificacao_farmacias,
    buscar_versoes_farmacias,
    salvar_comparacao,
    salvar_status_farmacias,
)
from app.repositories.redshift_repository import (
    execute_gold_vendas,
    execute_dimensao_por_codigos,
)
from app.utils import camadas_atrasadas

logger = logging.getLogger(__name__)


# ── Mappers ────────────────────────────────────────────────────────────────────

def montar_divergencia_response(d, camadas_atrasadas_list, camadas_sem_dados, classificacao: str | None = None) -> DivergenciaResponse:
    return DivergenciaResponse(
        cod_farmacia=d.cod_farmacia,
        nome_farmacia=d.nome_farmacia,
        cnpj=d.cnpj,
        sit_contrato=d.sit_contrato,
        codigo_rede=d.codigo_rede,
        num_versao=d.num_versao,
        ultima_venda_GoldVendas=d.ultima_venda_GoldVendas,
        ultima_hora_venda_GoldVendas=d.ultima_hora_venda_GoldVendas,
        tipo_divergencia=d.tipo_divergencia,
        camadas_atrasadas=camadas_atrasadas_list,
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


def montar_comparacao_response(resultado, divergencias, status_farmacias) -> ComparacaoResponse:
    return ComparacaoResponse(
        associacao=resultado.associacao,
        total_gold_vendas=resultado.total_gold_vendas,
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


# ── Service ────────────────────────────────────────────────────────────────────

def _comparar_resultados(associacao: str) -> ResultadoComparacao:

    logger.info("🔍 Iniciando comparação — associacao=%s", associacao)
    t_total = time.perf_counter()

    resultados_gold_vendas = execute_gold_vendas(associacao)

    gold_by_farmacia = {str(r["cod_farmacia"]).strip(): r for r in resultados_gold_vendas}
    todas_farmacias = set(gold_by_farmacia.keys())

    divergencias = []

    for cod in todas_farmacias:
        r_gold = gold_by_farmacia.get(cod)

        if r_gold:
            divergencias.append(Divergencia(
                cod_farmacia=cod,
                nome_farmacia=r_gold.get("nome_farmacia"),
                cnpj=r_gold.get("cnpj"),
                sit_contrato=r_gold.get("sit_contrato"),
                codigo_rede=r_gold.get("codigo_rede"),
                num_versao=r_gold.get("num_versao"),
                ultima_venda_GoldVendas=str(r_gold.get("ultima_venda")) if r_gold.get("ultima_venda") else None,
                ultima_hora_venda_GoldVendas=str(r_gold.get("ultima_hora_venda")) if r_gold.get("ultima_hora_venda") else None,
                tipo_divergencia="apenas_gold_vendas",
            ))

    resultado = ResultadoComparacao(
        associacao=associacao,
        total_gold_vendas=len(resultados_gold_vendas),
        total_divergencias=len(divergencias),
        divergencias=divergencias,
        todas_farmacias=todas_farmacias,
        resultados_gold_vendas=resultados_gold_vendas,
    )

    logger.info(
        "🏁 Comparação concluída em %.2fs — gold=%d, divergências=%d",
        time.perf_counter() - t_total,
        resultado.total_gold_vendas,
        resultado.total_divergencias,
    )
    return resultado


def executar_comparacao(associacao: str) -> ComparacaoResponse:
    logger.info("📥 Comparação iniciada — associacao=%s", associacao)
    t_req = time.perf_counter()

    resultado = _comparar_resultados(associacao)

    logger.info(
        "⏳ Buscando status Business Connect, Coletor BI e Sicfarma em paralelo (%d farmácias)",
        len(resultado.todas_farmacias),
    )
    t_parallel = time.perf_counter()

    with ThreadPoolExecutor(max_workers=4) as executor:
        future_bc = executor.submit(buscar_status_farmacias, list(resultado.todas_farmacias))
        future_sicfarma = executor.submit(buscar_classificacao_farmacias, list(resultado.todas_farmacias))
        future_versoes = executor.submit(buscar_versoes_farmacias, list(resultado.todas_farmacias))

        try:
            status_dict = future_bc.result()
        except Exception as e:
            logger.warning(
                "Business Connect (Coletor) indisponível (%.2fs): %s: %s",
                time.perf_counter() - t_parallel,
                type(e).__name__,
                e,
            )
            status_dict = {cod: "Indisponível" for cod in resultado.todas_farmacias}

        resultado_coletor = {}  # coletor_bi desativado

        try:
            classificacao_dict = future_sicfarma.result()
        except Exception as e:
            logger.warning(
                "Sicfarma indisponível (%.2fs): %s: %s",
                time.perf_counter() - t_parallel,
                type(e).__name__,
                e,
            )
            classificacao_dict = {cod: None for cod in resultado.todas_farmacias}

        try:
            versoes_dict = future_versoes.result()
        except Exception as e:
            logger.warning(
                "Sicfarma /versoes indisponível (%.2fs): %s: %s",
                time.perf_counter() - t_parallel,
                type(e).__name__,
                e,
            )
            versoes_dict = {cod: None for cod in resultado.todas_farmacias}

    logger.info("✅ APIs externas consultadas em %.2fs", time.perf_counter() - t_parallel)

    for d in resultado.divergencias:
        d.num_versao = versoes_dict.get(d.cod_farmacia)
    for r in resultado.resultados_gold_vendas:
        cod = str(r.get("cod_farmacia", "")).strip()
        r["num_versao"] = versoes_dict.get(cod)

    try:
        divergencias_dict = [
            {
                "cod_farmacia": d.cod_farmacia,
                "nome_farmacia": d.nome_farmacia,
                "cnpj": d.cnpj,
                "sit_contrato": d.sit_contrato,
                "codigo_rede": d.codigo_rede,
                "num_versao": d.num_versao,
                "ultima_venda_GoldVendas": d.ultima_venda_GoldVendas,
                "ultima_hora_venda_GoldVendas": d.ultima_hora_venda_GoldVendas,
                "tipo_divergencia": d.tipo_divergencia,
            }
            for d in resultado.divergencias
        ]
        resultado.comparacao_id = salvar_comparacao(
            associacao,
            resultado.resultados_gold_vendas,
            [],  # silver desativado
            divergencias_dict,
        )
    except Exception as e:
        logger.warning("Erro ao salvar comparação (não crítico): %s: %s", type(e).__name__, e)

    if resultado.comparacao_id is not None:
        try:
            salvar_status_farmacias(resultado.comparacao_id, resultado.associacao, status_dict, None, classificacao_dict)
        except Exception as e:
            logger.warning("Erro ao salvar status_farmacias: %s: %s", type(e).__name__, e)

    divergencias = []
    for d in resultado.divergencias:
        c_atrasadas, c_sem_dados = camadas_atrasadas(
            d.ultima_venda_GoldVendas,
            None,  # silver desativado
            status_dict.get(d.cod_farmacia),
        )
        classificacao = classificacao_dict.get(d.cod_farmacia)
        divergencias.append(montar_divergencia_response(d, c_atrasadas, c_sem_dados, classificacao))

    todas_farmacias = set(status_dict.keys())
    status_farmacias = [
        montar_status_farmacia_response(cod, status_dict.get(cod, "Indisponível"))
        for cod in sorted(todas_farmacias)
    ]

    response = montar_comparacao_response(resultado, divergencias, status_farmacias)

    logger.info("🚀 Comparação finalizada em %.2fs", time.perf_counter() - t_req)
    return response

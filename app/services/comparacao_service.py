"""Service de comparação entre GoldVendas e SilverSTGN_Dedup."""
import logging
import time
from concurrent.futures import ThreadPoolExecutor

from app.models.comparacao import Divergencia, ResultadoComparacao

from app.repositories.comparacao_repository import (
    buscar_por_associacao,
    buscar_status_farmacias,
    buscar_classificacao_farmacias,
    buscar_versoes_farmacias,
    salvar_comparacao,
    salvar_status_farmacias,
)
from app.repositories.redshift_repository import (
    execute_gold_vendas,
    # execute_silver_stgn_dedup,
    execute_dimensao_por_codigos,
)
from app.mappers.comparacao_mapper import (
    montar_comparacao_response,
    montar_divergencia_response,
    montar_status_farmacia_response,
)
from app.schemas import ComparacaoResponse
from app.utils import camadas_atrasadas

logger = logging.getLogger(__name__)


def _comparar_resultados(associacao: str) -> ResultadoComparacao:

    logger.info("🔍 Iniciando comparação — associacao=%s", associacao)
    t_total = time.perf_counter()

    resultados_gold_vendas = execute_gold_vendas(associacao)
    # resultados_silver_stgn_dedup = execute_silver_stgn_dedup(associacao)

    gold_by_farmacia = {str(r["cod_farmacia"]).strip(): r for r in resultados_gold_vendas}
    # silver_by_farmacia = {str(r["cod_farmacia"]).strip(): r for r in resultados_silver_stgn_dedup}

    todas_farmacias = set(gold_by_farmacia.keys())
    # | set(silver_by_farmacia.keys())

    # Lookup cadastral para farmácias silver-only (nome, cnpj, sit_contrato, codigo_rede)
    # silver_only_codes = [cod for cod in silver_by_farmacia if cod not in gold_by_farmacia]
    # cadfilia_lookup = execute_dimensao_por_codigos(silver_only_codes)
    # logger.info(
    #     "cadfilia lookup — silver_only_codes=%d, encontrados=%d | exemplos codes: %s",
    #     len(silver_only_codes),
    #     len(cadfilia_lookup),
    #     silver_only_codes[:5],
    # )

    divergencias = []

    for cod in todas_farmacias:
        r_gold = gold_by_farmacia.get(cod)
        # r_silver = silver_by_farmacia.get(cod)

        if r_gold and not r_silver:
            divergencias.append(Divergencia.from_gold_silver(cod, r_gold, None, "apenas_gold_vendas"))
        elif r_silver and not r_gold:
            cadfilia = cadfilia_lookup.get(cod, {})
            divergencias.append(Divergencia.from_gold_silver(cod, None, r_silver, "apenas_silver_stgn_dedup", cadfilia))
        else:
            venda_gold = str(r_gold.get("ultima_venda")) if r_gold and r_gold.get("ultima_venda") else None
            venda_silver = str(r_silver.get("ultima_venda")) if r_silver and r_silver.get("ultima_venda") else None

            if venda_gold != venda_silver:
                divergencias.append(Divergencia.from_gold_silver(cod, r_gold, r_silver, "data_diferente"))

    resultado = ResultadoComparacao(
        associacao=associacao,
        total_gold_vendas=len(resultados_gold_vendas),
        # total_silver_stgn_dedup=len(resultados_silver_stgn_dedup),  # silver desativado
        total_divergencias=len(divergencias),
        divergencias=divergencias,
        todas_farmacias=todas_farmacias,
        resultados_gold_vendas=resultados_gold_vendas,
        # resultados_silver_stgn_dedup=resultados_silver_stgn_dedup,  # silver desativado
    )

    logger.info(
        "🏁 Comparação concluída em %.2fs — gold=%d, divergências=%d",
        time.perf_counter() - t_total,
        resultado.total_gold_vendas,
        # resultado.total_silver_stgn_dedup,  # silver desativado
        resultado.total_divergencias,
    )
    return resultado


def _buscar_apis_externas(farmacias: list[str]) -> tuple[dict, dict, dict, dict]:
    """Consulta Business Connect, Coletor BI, Sicfarma e versões com executor compartilhado.

    Usa um único ThreadPoolExecutor para todas as chamadas por farmácia,
    evitando pools aninhados e limitando o total de threads.

    Returns:
        Tupla (status_dict, resultado_coletor, classificacao_dict, versoes_dict)
        com fallback seguro em caso de indisponibilidade de qualquer API.
    """
    logger.info(
        "⏳ Buscando status Business Connect, Coletor BI e Sicfarma em paralelo (%d farmácias)",
        len(farmacias),
    )
    t_parallel = time.perf_counter()

    def _safe_result(future, label: str, fallback):
        """Coleta resultado de um future com fallback seguro em caso de erro."""
        try:
            return future.result()
        except Exception as e:
            logger.warning("%s indisponível (%.2fs): %s: %s", label, time.perf_counter() - t_parallel, type(e).__name__, e)
            return fallback() if callable(fallback) else fallback

    with ThreadPoolExecutor(max_workers=10) as shared_executor:
        future_bc = shared_executor.submit(buscar_status_farmacias, farmacias, shared_executor)
        future_coletor = shared_executor.submit(buscar_por_associacao, farmacias, shared_executor)
        future_sicfarma = shared_executor.submit(buscar_classificacao_farmacias, farmacias, shared_executor)
        future_versoes = shared_executor.submit(buscar_versoes_farmacias, farmacias, shared_executor)

        status_dict = _safe_result(future_bc, "Business Connect", lambda: {cod: "Indisponível" for cod in farmacias})
        resultado_coletor = _safe_result(future_coletor, "Coletor BI", {})
        classificacao_dict = _safe_result(future_sicfarma, "Sicfarma", lambda: {cod: None for cod in farmacias})
        versoes_dict = _safe_result(future_versoes, "Sicfarma /versoes", lambda: {cod: None for cod in farmacias})

    logger.info("✅ APIs externas consultadas em %.2fs", time.perf_counter() - t_parallel)
    return status_dict, resultado_coletor, classificacao_dict, versoes_dict


def _aplicar_versoes(resultado: ResultadoComparacao, versoes_dict: dict) -> None:
    """Aplica num_versao do Sicfarma nas divergências e nos resultados gold."""
    for d in resultado.divergencias:
        d.num_versao = versoes_dict.get(d.cod_farmacia)
    for r in resultado.resultados_gold_vendas:
        cod = str(r.get("cod_farmacia", "")).strip()
        r["num_versao"] = versoes_dict.get(cod)


def _persistir_resultados(
    associacao: str,
    resultado: ResultadoComparacao,
    status_dict: dict,
    resultado_coletor: dict,
    classificacao_dict: dict,
) -> None:
    """Persiste comparação + status no banco local (não crítico — falhas são logadas)."""
    try:
        divergencias_dict = [d.to_dict() for d in resultado.divergencias]
        resultado.comparacao_id = salvar_comparacao(
            associacao,
            resultado.resultados_gold_vendas,
            [],  # silver desativado (resultados_silver_stgn_dedup)
            divergencias_dict,
        )
    except Exception as e:
        logger.warning("Erro ao salvar comparação (não crítico): %s: %s", type(e).__name__, e)

    if resultado.comparacao_id is not None:
        try:
            salvar_status_farmacias(resultado.comparacao_id, resultado.associacao, status_dict, None, classificacao_dict)  # coletor_bi desativado
        except Exception as e:
            logger.warning("Erro ao salvar status_farmacias: %s: %s", type(e).__name__, e)


def _montar_response(
    resultado: ResultadoComparacao,
    status_dict: dict,
    resultado_coletor: dict,
    classificacao_dict: dict,
) -> ComparacaoResponse:
    """Monta o ComparacaoResponse final com divergências e status de farmácias."""
    divergencias = []
    for d in resultado.divergencias:
        c_atrasadas, c_sem_dados = camadas_atrasadas(
            d.ultima_venda_GoldVendas,
            None,  # silver desativado (ultima_venda_SilverSTGN_Dedup)
            status_dict.get(d.cod_farmacia),
        )
        classificacao = classificacao_dict.get(d.cod_farmacia)
        divergencias.append(montar_divergencia_response(d, c_atrasadas, c_sem_dados, classificacao))

    todas_farmacias = set(status_dict.keys()) | set(resultado_coletor.keys())
    status_farmacias = [
        montar_status_farmacia_response(cod, status_dict.get(cod, "Indisponível"))  # coletor_bi desativado
        for cod in sorted(todas_farmacias)
    ]

    return montar_comparacao_response(resultado, divergencias, status_farmacias)


def executar_comparacao(associacao: str) -> ComparacaoResponse:
    """Orquestra o fluxo completo de comparação entre GoldVendas e SilverSTGN_Dedup."""
    logger.info("📥 Comparação iniciada — associacao=%s", associacao)
    t_req = time.perf_counter()

    resultado = _comparar_resultados(associacao)

    status_dict, resultado_coletor, classificacao_dict, versoes_dict = (
        _buscar_apis_externas(list(resultado.todas_farmacias))
    )

    _aplicar_versoes(resultado, versoes_dict)

    _persistir_resultados(associacao, resultado, status_dict, resultado_coletor, classificacao_dict)

    response = _montar_response(resultado, status_dict, resultado_coletor, classificacao_dict)

    logger.info("🚀 Comparação finalizada em %.2fs", time.perf_counter() - t_req)
    return response
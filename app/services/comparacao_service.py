import logging
import time
from concurrent.futures import ThreadPoolExecutor

# from app.clients.coletor_bi import buscar_por_associacao
from app.models.comparacao import Divergencia, ResultadoComparacao

from app.repositories.comparacao_repository import (
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
    """Executa as queries no Redshift e compara os resultados por cod_farmacia.

    Busca a última venda registrada por farmácia em cada fonte (sem filtro de data)
    e identifica 3 tipos de divergência em ultima_venda:
    - data_diferente: presente em ambas mas com datas distintas
    - apenas_gold_vendas: presente somente em associacao.vendas
    - apenas_silver_stgn_dedup: presente somente em silver.cadcvend_staging_dedup
    """
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

        if (r_gold):
                # and not r_silver):
            divergencias.append(Divergencia(
                cod_farmacia=cod,
                nome_farmacia=r_gold.get("nome_farmacia"),
                cnpj=r_gold.get("cnpj"),
                sit_contrato=r_gold.get("sit_contrato"),
                codigo_rede=r_gold.get("codigo_rede"),
                num_versao=r_gold.get("num_versao"),
                ultima_venda_GoldVendas=str(r_gold.get("ultima_venda")) if r_gold.get("ultima_venda") else None,
                ultima_hora_venda_GoldVendas=str(r_gold.get("ultima_hora_venda")) if r_gold.get("ultima_hora_venda") else None,
                # ultima_venda_SilverSTGN_Dedup=None,  # silver desativado
                # ultima_hora_venda_SilverSTGN_Dedup=None,  # silver desativado
                tipo_divergencia="apenas_gold_vendas",
            ))
        # elif r_silver and not r_gold:  # silver desativado
        #     cadfilia = cadfilia_lookup.get(cod, {})
        #     divergencias.append(Divergencia(
        #         cod_farmacia=cod,
        #         nome_farmacia=cadfilia.get("nome_farmacia"),
        #         cnpj=cadfilia.get("cnpj"),
        #         sit_contrato=cadfilia.get("sit_contrato"),
        #         codigo_rede=cadfilia.get("codigo_rede"),
        #         ultima_venda_GoldVendas=None,
        #         ultima_hora_venda_GoldVendas=None,
        #         ultima_venda_SilverSTGN_Dedup=str(r_silver.get("ultima_venda")) if r_silver.get("ultima_venda") else None,
        #         ultima_hora_venda_SilverSTGN_Dedup=str(r_silver.get("ultima_hora_venda")) if r_silver.get("ultima_hora_venda") else None,
        #         tipo_divergencia="apenas_silver_stgn_dedup",
        #     ))
        # else:  # silver desativado
        #     venda_gold = str(r_gold.get("ultima_venda")) if r_gold and r_gold.get("ultima_venda") else None
        #     venda_silver = str(r_silver.get("ultima_venda")) if r_silver and r_silver.get("ultima_venda") else None
        #
        #     if venda_gold != venda_silver:
        #         divergencias.append(Divergencia(
        #             cod_farmacia=cod,
        #             nome_farmacia=r_gold.get("nome_farmacia"),
        #             cnpj=r_gold.get("cnpj"),
        #             sit_contrato=r_gold.get("sit_contrato"),
        #             codigo_rede=r_gold.get("codigo_rede"),
        #             num_versao=r_gold.get("num_versao"),
        #             ultima_venda_GoldVendas=venda_gold,
        #             ultima_hora_venda_GoldVendas=str(r_gold.get("ultima_hora_venda")) if r_gold and r_gold.get("ultima_hora_venda") else None,
        #             ultima_venda_SilverSTGN_Dedup=venda_silver,
        #             ultima_hora_venda_SilverSTGN_Dedup=str(r_silver.get("ultima_hora_venda")) if r_silver and r_silver.get("ultima_hora_venda") else None,
        #             tipo_divergencia="data_diferente",
        #         ))

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


def executar_comparacao(associacao: str) -> ComparacaoResponse:
    logger.info("📥 Comparação iniciada — associacao=%s", associacao)
    t_req = time.perf_counter()

    # 1. Comparação Redshift
    resultado = _comparar_resultados(associacao)

    # 2. Business Connect + Coletor BI + Sicfarma em paralelo
    logger.info(
        "⏳ Buscando status Business Connect, Coletor BI e Sicfarma em paralelo (%d farmácias)",
        len(resultado.todas_farmacias),
    )
    t_parallel = time.perf_counter()

    with ThreadPoolExecutor(max_workers=4) as executor:
        future_bc = executor.submit(buscar_status_farmacias, list(resultado.todas_farmacias))
        # future_coletor = executor.submit(buscar_por_associacao, list(resultado.todas_farmacias))
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

        # try:
        #     resultado_coletor = future_coletor.result()
        # except Exception as e:
        #     logger.warning(
        #         "Coletor BI indisponivel (%.2fs): %s: %s",
        #         time.perf_counter() - t_parallel,
        #         type(e).__name__,
        #         e,
        #     )
        #     resultado_coletor = {}

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

    # Aplicar num_versao do Sicfarma nas divergências e nos resultados gold (antes de persistir)
    for d in resultado.divergencias:
        d.num_versao = versoes_dict.get(d.cod_farmacia)
    for r in resultado.resultados_gold_vendas:
        cod = str(r.get("cod_farmacia", "")).strip()
        r["num_versao"] = versoes_dict.get(cod)

    # 4. Persistência
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
                # "ultima_venda_SilverSTGN_Dedup": d.ultima_venda_SilverSTGN_Dedup,  # silver desativado
                # "ultima_hora_venda_SilverSTGN_Dedup": d.ultima_hora_venda_SilverSTGN_Dedup,  # silver desativado
                "tipo_divergencia": d.tipo_divergencia,
            }
            for d in resultado.divergencias
        ]
        resultado.comparacao_id = salvar_comparacao(
            associacao,
            resultado.resultados_gold_vendas,
            [],  # silver desativado (resultados_silver_stgn_dedup)
            divergencias_dict,
        )
    except Exception as e:
        logger.warning("Erro ao salvar comparação (não crítico): %s: %s", type(e).__name__, e)

    # 5. Persistir status Business Connect + Coletor BI
    if resultado.comparacao_id is not None:
        try:
            salvar_status_farmacias(resultado.comparacao_id, resultado.associacao, status_dict, None, classificacao_dict)  # coletor_bi desativado
        except Exception as e:
            logger.warning("Erro ao salvar status_farmacias: %s: %s", type(e).__name__, e)

    # 6. Montar divergências
    divergencias = []
    for d in resultado.divergencias:
        c_atrasadas, c_sem_dados = camadas_atrasadas(
            d.ultima_venda_GoldVendas,
            None,  # silver desativado (ultima_venda_SilverSTGN_Dedup)
            status_dict.get(d.cod_farmacia),
        )
        classificacao = classificacao_dict.get(d.cod_farmacia)
        divergencias.append(montar_divergencia_response(d, c_atrasadas, c_sem_dados, classificacao))

    # 7. Status farmácias
    todas_farmacias = set(status_dict.keys())  # coletor_bi desativado
    status_farmacias = [
        montar_status_farmacia_response(cod, status_dict.get(cod, "Indisponível"))  # coletor_bi desativado
        for cod in sorted(todas_farmacias)
    ]

    # 8. Response final
    response = montar_comparacao_response(resultado, divergencias, status_farmacias)

    logger.info("🚀 Comparação finalizada em %.2fs", time.perf_counter() - t_req)
    return response
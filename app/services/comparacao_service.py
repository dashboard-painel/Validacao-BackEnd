import logging
import time

from app.clients.coletor_bi import buscar_por_associacao
from app.models.comparacao import Divergencia, ResultadoComparacao

from app.repositories.comparacao_repository import (
    buscar_status_farmacias,
    salvar_comparacao,
    salvar_status_farmacias,
)
from app.repositories.redshift_repository import (
    execute_gold_vendas,
    execute_silver_stgn_dedup,
    execute_cadfilia_por_codigos,
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
    resultados_silver_stgn_dedup = execute_silver_stgn_dedup(associacao)

    gold_by_farmacia = {str(r["cod_farmacia"]).strip(): r for r in resultados_gold_vendas}
    silver_by_farmacia = {str(r["cod_farmacia"]).strip(): r for r in resultados_silver_stgn_dedup}

    todas_farmacias = set(gold_by_farmacia.keys()) | set(silver_by_farmacia.keys())

    # Lookup cadastral para farmácias silver-only (nome, cnpj, sit_contrato, codigo_rede)
    silver_only_codes = [cod for cod in silver_by_farmacia if cod not in gold_by_farmacia]
    cadfilia_lookup = execute_cadfilia_por_codigos(silver_only_codes)
    logger.info(
        "cadfilia lookup — silver_only_codes=%d, encontrados=%d | exemplos codes: %s",
        len(silver_only_codes),
        len(cadfilia_lookup),
        silver_only_codes[:5],
    )

    # Fallback de sit_contrato para farmácias gold com valor nulo na dimensao
    gold_sem_contrato = [cod for cod, r in gold_by_farmacia.items() if not r.get("sit_contrato")]
    if gold_sem_contrato:
        gold_fallback = execute_cadfilia_por_codigos(gold_sem_contrato)
        for cod, dados in gold_fallback.items():
            if dados.get("sit_contrato") and cod in gold_by_farmacia:
                gold_by_farmacia[cod]["sit_contrato"] = dados["sit_contrato"]
        logger.info(
            "fallback sit_contrato gold — %d farmácias consultadas, %d preenchidas",
            len(gold_sem_contrato),
            sum(1 for cod in gold_sem_contrato if gold_by_farmacia.get(cod, {}).get("sit_contrato")),
        )

    divergencias = []

    for cod in todas_farmacias:
        r_gold = gold_by_farmacia.get(cod)
        r_silver = silver_by_farmacia.get(cod)

        if r_gold and not r_silver:
            divergencias.append(Divergencia(
                cod_farmacia=cod,
                nome_farmacia=r_gold.get("nome_farmacia"),
                cnpj=r_gold.get("cnpj"),
                sit_contrato=r_gold.get("sit_contrato"),
                codigo_rede=r_gold.get("codigo_rede"),
                ultima_venda_GoldVendas=str(r_gold.get("ultima_venda")) if r_gold.get("ultima_venda") else None,
                ultima_hora_venda_GoldVendas=str(r_gold.get("ultima_hora_venda")) if r_gold.get("ultima_hora_venda") else None,
                ultima_venda_SilverSTGN_Dedup=None,
                ultima_hora_venda_SilverSTGN_Dedup=None,
                tipo_divergencia="apenas_gold_vendas",
            ))
        elif r_silver and not r_gold:
            cadfilia = cadfilia_lookup.get(cod, {})
            divergencias.append(Divergencia(
                cod_farmacia=cod,
                nome_farmacia=cadfilia.get("nome_farmacia"),
                cnpj=cadfilia.get("cnpj"),
                sit_contrato=cadfilia.get("sit_contrato"),
                codigo_rede=cadfilia.get("codigo_rede"),
                ultima_venda_GoldVendas=None,
                ultima_hora_venda_GoldVendas=None,
                ultima_venda_SilverSTGN_Dedup=str(r_silver.get("ultima_venda")) if r_silver.get("ultima_venda") else None,
                ultima_hora_venda_SilverSTGN_Dedup=str(r_silver.get("ultima_hora_venda")) if r_silver.get("ultima_hora_venda") else None,
                tipo_divergencia="apenas_silver_stgn_dedup",
            ))
        else:
            venda_gold = str(r_gold.get("ultima_venda")) if r_gold and r_gold.get("ultima_venda") else None
            venda_silver = str(r_silver.get("ultima_venda")) if r_silver and r_silver.get("ultima_venda") else None

            if venda_gold != venda_silver:
                nome = r_gold.get("nome_farmacia") if r_gold else None
                cnpj = r_gold.get("cnpj") if r_gold else None
                sit_contrato = r_gold.get("sit_contrato") if r_gold else None
                codigo_rede = r_gold.get("codigo_rede") if r_gold else None
                divergencias.append(Divergencia(
                    cod_farmacia=cod,
                    nome_farmacia=nome,
                    cnpj=cnpj,
                    sit_contrato=sit_contrato,
                    codigo_rede=codigo_rede,
                    ultima_venda_GoldVendas=venda_gold,
                    ultima_hora_venda_GoldVendas=str(r_gold.get("ultima_hora_venda")) if r_gold and r_gold.get("ultima_hora_venda") else None,
                    ultima_venda_SilverSTGN_Dedup=venda_silver,
                    ultima_hora_venda_SilverSTGN_Dedup=str(r_silver.get("ultima_hora_venda")) if r_silver and r_silver.get("ultima_hora_venda") else None,
                    tipo_divergencia="data_diferente",
                ))

    resultado = ResultadoComparacao(
        associacao=associacao,
        total_gold_vendas=len(resultados_gold_vendas),
        total_silver_stgn_dedup=len(resultados_silver_stgn_dedup),
        total_divergencias=len(divergencias),
        divergencias=divergencias,
        todas_farmacias=todas_farmacias,
        resultados_gold_vendas=resultados_gold_vendas,
        resultados_silver_stgn_dedup=resultados_silver_stgn_dedup,
    )

    logger.info(
        "🏁 Comparação concluída em %.2fs — gold=%d, silver=%d, divergências=%d",
        time.perf_counter() - t_total,
        resultado.total_gold_vendas,
        resultado.total_silver_stgn_dedup,
        resultado.total_divergencias,
    )
    return resultado


def executar_comparacao(associacao: str) -> ComparacaoResponse:
    logger.info("📥 Comparação iniciada — associacao=%s", associacao)
    t_req = time.perf_counter()

    # 1. Comparação Redshift
    resultado = _comparar_resultados(associacao)

    # 2. Business Connect
    logger.info("⏳ Buscando status Business Connect (%d farmácias)", len(resultado.todas_farmacias))
    t_bc = time.perf_counter()

    try:
        status_dict = buscar_status_farmacias(list(resultado.todas_farmacias))
        logger.info("✅ Business Connect respondeu em %.2fs", time.perf_counter() - t_bc)

    except Exception as e:
        logger.warning(
            "Business Connect indisponível (%.2fs): %s: %s",
            time.perf_counter() - t_bc,
            type(e).__name__,
            e,
        )
        status_dict = {cod: "Indisponível" for cod in resultado.todas_farmacias}

    # 3. Coletor BI
    logger.info("⏳ Buscando status Coletor BI (%d farmácias)", len(resultado.todas_farmacias))
    t_coletor = time.perf_counter()

    try:
        resultado_coletor = buscar_por_associacao(list(resultado.todas_farmacias))
        logger.info("✅ Coletor BI respondeu em %.2fs", time.perf_counter() - t_coletor)

    except Exception as e:
        logger.warning(
            "Coletor BI indisponivel (%.2fs): %s: %s",
            time.perf_counter() - t_coletor,
            type(e).__name__,
            e,
        )
        resultado_coletor = {cod: "Indisponivel" for cod in resultado.todas_farmacias}

    # 4. Persistência
    try:
        divergencias_dict = [
            {
                "cod_farmacia": d.cod_farmacia,
                "nome_farmacia": d.nome_farmacia,
                "cnpj": d.cnpj,
                "sit_contrato": d.sit_contrato,
                "codigo_rede": d.codigo_rede,
                "ultima_venda_GoldVendas": d.ultima_venda_GoldVendas,
                "ultima_hora_venda_GoldVendas": d.ultima_hora_venda_GoldVendas,
                "ultima_venda_SilverSTGN_Dedup": d.ultima_venda_SilverSTGN_Dedup,
                "ultima_hora_venda_SilverSTGN_Dedup": d.ultima_hora_venda_SilverSTGN_Dedup,
                "tipo_divergencia": d.tipo_divergencia,
            }
            for d in resultado.divergencias
        ]
        resultado.comparacao_id = salvar_comparacao(
            associacao,
            resultado.resultados_gold_vendas,
            resultado.resultados_silver_stgn_dedup,
            divergencias_dict,
        )
    except Exception as e:
        logger.warning("Erro ao salvar comparação (não crítico): %s: %s", type(e).__name__, e)

    # 5. Persistir status Business Connect + Coletor BI
    if resultado.comparacao_id is not None:
        try:
            salvar_status_farmacias(resultado.comparacao_id, resultado.associacao, status_dict, resultado_coletor)
        except Exception as e:
            logger.warning("Erro ao salvar status_farmacias: %s: %s", type(e).__name__, e)

    # 6. Montar divergências
    divergencias = []
    for d in resultado.divergencias:
        c_atrasadas, c_sem_dados = camadas_atrasadas(
            d.ultima_venda_GoldVendas,
            d.ultima_venda_SilverSTGN_Dedup,
            status_dict.get(d.cod_farmacia),
        )
        divergencias.append(montar_divergencia_response(d, c_atrasadas, c_sem_dados))

    # 7. Status farmácias
    todas_farmacias = set(status_dict.keys()) | set(resultado_coletor.keys())
    status_farmacias = [
        montar_status_farmacia_response(cod, status_dict.get(cod, "Indisponível"), resultado_coletor.get(cod))
        for cod in sorted(todas_farmacias)
    ]

    # 8. Response final
    response = montar_comparacao_response(resultado, divergencias, status_farmacias)

    logger.info("🚀 Comparação finalizada em %.2fs", time.perf_counter() - t_req)
    return response
"""Router para endpoint de comparação de dados."""
import logging
import time
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.business_connect import buscar_status_farmacias
from app.comparador import comparar_resultados
from app.local_db import buscar_todos_consolidados, buscar_consolidado_por_associacao, salvar_status_farmacias
from app.schemas import AssociacaoResumoResponse, ComparacaoRequest, ComparacaoResponse, DivergenciaResponse, FarmaciaStatusResponse, ResultadoConsolidadoResponse

logger = logging.getLogger(__name__)

router = APIRouter(tags=["comparação"])


def _camadas_atrasadas(
    data_gold: Optional[str],
    data_silver: Optional[str],
    coletor_novo: Optional[str],
) -> tuple[Optional[list[str]], Optional[list[str]]]:
    """Retorna (camadas_atrasadas, camadas_sem_dados).

    camadas_atrasadas — tem dado mas é velho:
    - GoldVendas / SilverSTGN_Dedup / API: data < D-1 (apenas D-2 ou mais antigo)

    camadas_sem_dados — sem nenhum registro:
    - GoldVendas / SilverSTGN_Dedup: campo null
    """
    ontem = date.today() - timedelta(days=1)
    atrasadas: list[str] = []
    sem_dados: list[str] = []

    for campo, d_str in [("GoldVendas", data_gold), ("SilverSTGN_Dedup", data_silver)]:
        if not d_str:
            sem_dados.append(campo)
        else:
            try:
                if date.fromisoformat(str(d_str)[:10]) < ontem:
                    atrasadas.append(campo)
            except ValueError:
                pass

    if coletor_novo and coletor_novo.startswith("Pendente de envio no dia "):
        data_api = coletor_novo.removeprefix("Pendente de envio no dia ").strip()
        try:
            if date.fromisoformat(data_api[:10]) < ontem:
                atrasadas.append("API")
        except ValueError:
            pass

    return (atrasadas or None, sem_dados or None)


@router.get("/comparar", response_model=ComparacaoResponse)
async def comparar(
    associacao: str = Query(..., description="Código da associação para filtrar"),
) -> ComparacaoResponse:
    """Compara os resultados das duas queries do Redshift.

    Executa GoldVendas (associacao.vendas) e SilverSTGN_Dedup (silver.cadcvend_staging_dedup)
    e retorna a última venda registrada por farmácia, sem filtro de data.

    Tipos de divergência identificados:
    - **data_diferente**: farmácia presente em ambas mas com datas distintas
    - **apenas_gold_vendas**: farmácia presente somente em GoldVendas
    - **apenas_silver_stgn_dedup**: farmácia presente somente em SilverSTGN_Dedup

    Args:
        associacao: Código da associação (obrigatório)

    Returns:
        ComparacaoResponse com totais e lista de divergências
    """
    return _executar_comparacao(associacao)


def _executar_comparacao(associacao: str) -> ComparacaoResponse:
    """Lógica compartilhada entre GET e POST.

    Fluxo:
    1. Executa comparação Redshift via comparar_resultados()
    2. Consulta status de migração no Business Connect para TODAS as farmácias
    3. Persiste status no banco local (se comparação foi salva)
    4. Retorna resposta unificada
    """
    logger.info("📥 Requisição recebida — associacao=%s", associacao)
    t_req = time.perf_counter()

    # 1. Comparação Redshift
    try:
        resultado = comparar_resultados(associacao)
    except Exception as e:
        logger.error("Erro ao executar comparação: %s: %s", type(e).__name__, e)
        raise HTTPException(
            status_code=503,
            detail=f"Erro de conexão com o banco de dados. Tente novamente em alguns instantes. Detalhes: {type(e).__name__}",
        )

    # 2. Business Connect — status de migração de todas as farmácias (GoldVendas + SilverSTGN_Dedup)
    logger.info("⏳ Aguardando resposta Business Connect — %d farmácias...", len(resultado.todas_farmacias))
    t_bc = time.perf_counter()
    try:
        status_dict = buscar_status_farmacias(list(resultado.todas_farmacias))
        logger.info("✅ Business Connect respondeu em %.2fs", time.perf_counter() - t_bc)
    except Exception as e:
        logger.warning(
            "Business Connect indisponível (%.2fs) — usando fallback 'Indisponível': %s: %s",
            time.perf_counter() - t_bc,
            type(e).__name__,
            e,
        )
        status_dict = {cod: "Indisponível" for cod in resultado.todas_farmacias}

    # 3. Persistir status no banco local
    if resultado.comparacao_id is not None:
        try:
            salvar_status_farmacias(resultado.comparacao_id, resultado.associacao, status_dict)
        except Exception as e:
            logger.warning(
                "Erro ao salvar status_farmacias no banco local (não crítico): %s: %s",
                type(e).__name__,
                e,
            )

    # 4. Construir resposta
    divergencias_response = [
        DivergenciaResponse(
            cod_farmacia=d.cod_farmacia,
            nome_farmacia=d.nome_farmacia,
            cnpj=d.cnpj,
            ultima_venda_GoldVendas=d.ultima_venda_GoldVendas,
            ultima_hora_venda_GoldVendas=d.ultima_hora_venda_GoldVendas,
            ultima_venda_SilverSTGN_Dedup=d.ultima_venda_SilverSTGN_Dedup,
            ultima_hora_venda_SilverSTGN_Dedup=d.ultima_hora_venda_SilverSTGN_Dedup,
            tipo_divergencia=d.tipo_divergencia,
            **dict(zip(
                ["camadas_atrasadas", "camadas_sem_dados"],
                _camadas_atrasadas(d.ultima_venda_GoldVendas, d.ultima_venda_SilverSTGN_Dedup, status_dict.get(d.cod_farmacia)),
            )),
        )
        for d in resultado.divergencias
    ]

    status_farmacias_response = [
        FarmaciaStatusResponse(cod_farmacia=cod, coletor_novo=status)
        for cod, status in status_dict.items()
    ]

    response = ComparacaoResponse(
        associacao=resultado.associacao,
        total_gold_vendas=resultado.total_gold_vendas,
        total_silver_stgn_dedup=resultado.total_silver_stgn_dedup,
        total_divergencias=resultado.total_divergencias,
        comparacao_id=resultado.comparacao_id,
        divergencias=divergencias_response,
        status_farmacias=status_farmacias_response,
    )
    logger.info("🚀 Requisição concluída em %.2fs", time.perf_counter() - t_req)
    return response


@router.post("/comparar", response_model=ComparacaoResponse)
async def comparar_post(body: ComparacaoRequest) -> ComparacaoResponse:
    """Compara os resultados das duas queries via body JSON.

    Mesma lógica do GET /comparar, mas recebe os parâmetros no corpo da requisição.

    Args:
        body: JSON com `associacao` (obrigatório)

    Returns:
        ComparacaoResponse com totais e lista de divergências

    Raises:
        HTTPException 503: Erro de conexão com o Redshift
    """
    return _executar_comparacao(body.associacao)


@router.get("/historico", response_model=list[ResultadoConsolidadoResponse])
async def listar_todas_farmacias() -> list[ResultadoConsolidadoResponse]:
    """Retorna todas as farmácias de todas as associações (última comparação de cada uma).

    Ideal para a tela inicial do dashboard: carrega a tabela completa sem precisar
    especificar uma associação.

    Returns:
        Lista de todas as farmácias, ordenada por associacao e cod_farmacia,
        com coletor_novo e tipo_divergencia embutidos

    Raises:
        HTTPException 503: Erro de conexão com o banco local
    """
    try:
        rows = buscar_todos_consolidados()
    except Exception as e:
        logger.error("Erro ao buscar todos consolidados: %s: %s", type(e).__name__, e)
        raise HTTPException(
            status_code=503,
            detail=f"Erro ao acessar o banco local. Detalhes: {type(e).__name__}",
        )

    return [
        ResultadoConsolidadoResponse(
            associacao=row["associacao"],
            cod_farmacia=row["cod_farmacia"],
            nome_farmacia=row.get("nome_farmacia"),
            cnpj=row.get("cnpj"),
            ultima_venda_GoldVendas=row.get("ultima_venda_goldvendas"),
            ultima_hora_venda_GoldVendas=row.get("ultima_hora_venda_goldvendas"),
            ultima_venda_SilverSTGN_Dedup=row.get("ultima_venda_silverstgn_dedup"),
            ultima_hora_venda_SilverSTGN_Dedup=row.get("ultima_hora_venda_silverstgn_dedup"),
            coletor_novo=row.get("coletor_novo"),
            tipo_divergencia=row.get("tipo_divergencia"),
            **dict(zip(
                ["camadas_atrasadas", "camadas_sem_dados"],
                _camadas_atrasadas(row.get("ultima_venda_goldvendas"), row.get("ultima_venda_silverstgn_dedup"), row.get("coletor_novo")),
            )),
        )
        for row in rows
    ]


@router.get("/historico/{associacao}", response_model=list[ResultadoConsolidadoResponse])
async def historico_consolidado(associacao: str) -> list[ResultadoConsolidadoResponse]:
    """Retorna os resultados consolidados da comparação mais recente de uma associação.

    Inclui dados de ambas as fontes (GoldVendas + SilverSTGN_Dedup), status do
    coletor no Business Connect e tipo de divergência por farmácia.

    Args:
        associacao: Código da associação

    Returns:
        Lista de farmácias com dados consolidados, coletor_novo e tipo_divergencia

    Raises:
        HTTPException 404: Nenhuma comparação encontrada para a associação
        HTTPException 503: Erro de conexão com o banco local
    """
    try:
        rows = buscar_consolidado_por_associacao(associacao)
    except Exception as e:
        logger.error("Erro ao buscar historico para associacao=%s: %s: %s", associacao, type(e).__name__, e)
        raise HTTPException(
            status_code=503,
            detail=f"Erro ao acessar o banco local. Detalhes: {type(e).__name__}",
        )

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Nenhuma comparação encontrada para associacao={associacao}",
        )

    return [
        ResultadoConsolidadoResponse(
            associacao=associacao,
            cod_farmacia=row["cod_farmacia"],
            nome_farmacia=row.get("nome_farmacia"),
            cnpj=row.get("cnpj"),
            ultima_venda_GoldVendas=row.get("ultima_venda_goldvendas"),
            ultima_hora_venda_GoldVendas=row.get("ultima_hora_venda_goldvendas"),
            ultima_venda_SilverSTGN_Dedup=row.get("ultima_venda_silverstgn_dedup"),
            ultima_hora_venda_SilverSTGN_Dedup=row.get("ultima_hora_venda_silverstgn_dedup"),
            coletor_novo=row.get("coletor_novo"),
            tipo_divergencia=row.get("tipo_divergencia"),
            **dict(zip(
                ["camadas_atrasadas", "camadas_sem_dados"],
                _camadas_atrasadas(row.get("ultima_venda_goldvendas"), row.get("ultima_venda_silverstgn_dedup"), row.get("coletor_novo")),
            )),
        )
        for row in rows
    ]

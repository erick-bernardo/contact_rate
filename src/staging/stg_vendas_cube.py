import pandas as pd
from pathlib import Path

from src.config.settings import RAW_VENDAS_PATH
from src.utils.logger import setup_logger

logger = setup_logger()


def stg_vendas_cube():

    logger.info("===== STG VENDAS CUBE START =====")

    files = list(RAW_VENDAS_PATH.glob("*.parquet"))

    if not files:

        logger.error("Nenhum arquivo encontrado em RAW_VENDAS_PATH")

        raise ValueError("RAW vendas vazio")

    df_list = []

    for file in files:

        df = pd.read_parquet(file)

        df_list.append(df)

    df = pd.concat(df_list, ignore_index=True)

    logger.info(f"Linhas carregadas RAW vendas: {len(df)}")

    # =========================
    # Padronização de strings
    # =========================

    str_cols = [
        "empresa_venda",
        "tipo_venda",
        "status_pedido"
    ]    

    for col in str_cols:
        df[col] = (
            df[col]
            .astype("string")
            .str.strip()
            .str.lower()
        )

    # =================================
    # Tratamento da data da venda
    # =================================

    df["data_venda"] = pd.to_datetime(
        df["data_compra_cliente"],
        errors="coerce"
    ).dt.date

    # =================================
    # Criar grupo_venda
    # =================================

    df["grupo_venda"] = df["empresa_venda"].apply(
        lambda x: "mm"
        if x in ["mm", "mm_app", "guide shop", "casatema"]
        else "marketplaces"
    )

    # =================================
    # Filtro de situação da venda
    # =================================

    df["data_cancelamento"] = pd.to_datetime(
        df["data_cancelamento"],
        errors="coerce"
    ).dt.date

    df["data_aprovacao"] = pd.to_datetime(
        df["data_aprovacao"],
        errors="coerce"
    ).dt.date

    # Manter apenas pedidos que passaram por aprovação (data_aprovacao diferente de 1899-12-31 e 1900-01-01)
    data_nao_aprovados_1 = pd.to_datetime("1899-12-31").date()
    data_nao_aprovados_2 = pd.to_datetime("1900-01-01").date()
    df = df[~df["data_aprovacao"].isin([data_nao_aprovados_1, data_nao_aprovados_2])]

    # =================================
    # Criar dimensões de tempo
    # =================================

    df["data_venda_dt"] = pd.to_datetime(df["data_venda"])
    df["semana_venda"] = df["data_venda_dt"].dt.strftime("%G-W%V")
    df["mes_venda"] = df["data_venda_dt"].dt.strftime("%Y-%m")

    # =================================
    # Agregação métricas
    # =================================

    vendas_cube = (

        df.groupby(
            [
                "data_venda",
                "semana_venda",
                "mes_venda",
                "empresa_venda",
                "tipo_venda",
                "grupo_venda"
            ]
        )
        .agg(

            itens_vendidos=("id_pedido", "count"),

            pedidos_vendidos=("id_pedido", "nunique")

        )
        .reset_index()

    )

    logger.info(
        f"Linhas stg_vendas_cube: {len(vendas_cube)}"
    )

    # Listar semanas carregadas
    semanas_carregadas = vendas_cube["semana_venda"].unique()
    semanas_ordenadas = sorted(semanas_carregadas)
    logger.info(f"Semanas carregadas: {', '.join(semanas_ordenadas)}")

    logger.info("===== STG VENDAS CUBE END =====")

    return vendas_cube
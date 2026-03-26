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
        lambda x: "MM"
        if x in ["mm", "mm_app", "guideshop", "casatema"]
        else "marketplaces"
    )

    # =================================
    # Filtro de situação da venda
    # =================================

    df = df[df["situacao"] == "Aprovado"]

    # =================================
    # Agregação métricas
    # =================================

    vendas_cube = (

        df.groupby(
            [
                "data_venda",
                "empresa_venda",
                "tipo_venda",
                "grupo_venda"
            ]
        )
        .agg(

            itens_vendidos=("id_produto", "count"),

            pedidos_vendidos=("id_pedido", "nunique")

        )
        .reset_index()

    )

    logger.info(
        f"Linhas stg_vendas_cube: {len(vendas_cube)}"
    )

    logger.info("===== STG VENDAS CUBE END =====")

    return vendas_cube
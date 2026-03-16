import pandas as pd


def stg_vendas(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    # =========================
    # Seleção de colunas
    # =========================

    cols = [
        "id_pedido",
        "empresa_venda",
        "tipo_venda",
        "status_pedido",
        "data_compra_cliente",
        "data_entrega_cliente_revisada",
        "data_entrega",
        "data_entregue",
        "data_cancelamento"
    ]

    df = df[cols]

    # =========================
    # Tratamento de tipos
    # =========================

    df["id_pedido"] = pd.to_numeric(
        df["id_pedido"],
        errors="coerce"
    ).astype("Int64")

    date_cols = [
        "data_compra_cliente",
        "data_entrega_cliente_revisada",
        "data_entrega",
        "data_entregue",
        "data_cancelamento"
    ]

    for col in date_cols:
        df[col] = pd.to_datetime(
            df[col],
            errors="coerce"
        )

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

    # =========================
    # Remover registros sem pedido
    # =========================

    df = df.dropna(subset=["id_pedido"])

    # =========================
    # Garantir granularidade pedido
    # =========================

    df = df.drop_duplicates(subset=["id_pedido"])      

    return df
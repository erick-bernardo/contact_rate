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
        "data_cancelamento",
        "data_aprovacao",
        "data_entregue_cliente",
        "data_prometido_entrega_cliente",
        "situacao",
        "id_cliente",
        "cidade_entrega",
        "micro_regiao_entrega",
        "regiao_destino"
    ]

    df = df[cols]

    # =========================
    # Tratamento de tipos
    # =========================

    df["id_pedido"] = (
        df["id_pedido"]
        .astype("string")
        .str.replace(r'\D+', '', regex=True)
        .str.strip()
    )

    # Tratamento id_cliente (remover decimais)
    df["id_cliente"] = (
        df["id_cliente"]
        .astype("string")
        .str.replace(r'\D+', '', regex=True)
        .str.strip()
    )

    date_cols = [
        "data_compra_cliente",
        "data_entrega_cliente_revisada",
        "data_entrega",
        "data_entregue",
        "data_cancelamento",
        "data_aprovacao",
        "data_entregue_cliente",
        "data_prometido_entrega_cliente"
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
        "status_pedido",
        "situacao",
        "cidade_entrega",
        "micro_regiao_entrega",
        "regiao_destino"
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
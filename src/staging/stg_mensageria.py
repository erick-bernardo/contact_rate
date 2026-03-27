import pandas as pd


def stg_mensageria(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    # =========================
    # Rename de colunas
    # =========================

    df = df.rename(columns={
        "data_atendimento": "data_contato",
        "motivo_contato": "motivo_principal",
        "submotivo_contato": "submotivo_principal"
    })

    # =========================
    # Agregação de SKUs
    # =========================

    sku_agg = (
        df.groupby(["id_contato", "id_pedido"])["id_produto"]
        .apply(lambda x: ",".join(x.astype(str).unique()))
        .reset_index()
        .rename(columns={"id_produto": "sku_item_quantidade"})
    )

    # =========================
    # Reduz granularidade
    # =========================

    df = df.drop(columns=["id_produto"])

    df = df.drop_duplicates(
        subset=["id_contato", "id_pedido"]
    )

    df = df.merge(
        sku_agg,
        on=["id_contato", "id_pedido"],
        how="left"
    )

    # =========================
    # Colunas inexistentes
    # =========================

    df["seller"] = pd.NA
    df["via_channel"] = pd.NA
    df["tag_pcid"] = pd.NA
    df["retido_bot"] = "não passou pelo bot"

    # =========================
    # Seleção de colunas
    # =========================

    cols = [
        "id_contato",
        "data_contato",
        "id_pedido_pai",
        "id_pedido",
        "canal_atendimento",
        "seller",
        "motivo_principal",
        "submotivo_principal",
        "tipo_ticket",
        "status_ticket",
        "formulario",
        "via_channel",
        "tag_pcid",
        "retido_bot",
        "sku_item_quantidade"
    ]

    df = df[cols]

    # =========================
    # Tratamento de tipos
    # =========================

    df["id_contato"] = (
        df["id_contato"]
        .astype("string")
        .str.strip()
    )

    df["data_contato"] = pd.to_datetime(
        df["data_contato"],
        errors="coerce"
    )
    df["id_pedido_pai"] = (
        pd.to_numeric(df["id_pedido_pai"], errors="coerce")
        .astype("Int64", errors="ignore")
        .astype("string")
        .str.strip()
    )

    df["id_pedido"] = (
        pd.to_numeric(df["id_pedido"], errors="coerce")
        .astype("Int64", errors="ignore")
        .astype("string")
        .str.strip()
    )


    # df["id_pedido_pai"] = pd.to_numeric(
    #     df["id_pedido_pai"],
    #     errors="coerce"
    # ).astype("Int64")

    # df["id_pedido"] = pd.to_numeric(
    #     df["id_pedido"],
    #     errors="coerce"
    # ).astype("Int64")

    # =========================
    # Padronização de strings
    # =========================

    str_cols = [
        "canal_atendimento",
        "seller",
        "motivo_principal",
        "submotivo_principal",
        "tipo_ticket",
        "status_ticket",
        "formulario",
        "via_channel",
        "tag_pcid",
        "sku_item_quantidade",
        "retido_bot"
    ]

    for col in str_cols:
        df[col] = (
            df[col]
            .astype("string")
            .str.strip()
            .str.lower()
        )

    # =========================
    # Origem
    # =========================

    df["origem_contato"] = "mensageria seller"
    df["origem_contato"] = df["origem_contato"].astype("string")

    return df
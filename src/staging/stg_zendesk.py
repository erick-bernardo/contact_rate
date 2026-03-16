import pandas as pd


def stg_zendesk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Staging da base Zendesk.

    Responsabilidades:
    - renomear colunas
    - selecionar colunas necessárias
    - tratar tipos
    """

    df = df.copy()

    # =========================
    # Rename de colunas
    # =========================

    df = df.rename(columns={
        "ticket_id": "id_contato",
        "created_at": "data_contato",
        "pedido_pai": "id_pedido_pai",
        "pedido_filho": "id_pedido",
        "type": "tipo_ticket"
    })

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
        "retido_bot_mkt",
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
        errors="coerce",
        utc=False
    )

    df["id_pedido_pai"] = pd.to_numeric(
        df["id_pedido_pai"],
        errors="coerce"
    ).astype("Int64")

    df["id_pedido"] = pd.to_numeric(
        df["id_pedido"],
        errors="coerce"
    ).astype("Int64")

    # =========================
    # Strings padronizadas
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
        "retido_bot_mkt"
    ]

    for col in str_cols:
        df[col] = (
            df[col]
            .astype("string")
            .str.strip()
            .str.lower()
        )
    
    # =========================
    # Coluna origem
    # =========================

    df["origem_contato"] = "zendesk"
    df["origem_contato"] = df["origem_contato"].astype("string")

    return df
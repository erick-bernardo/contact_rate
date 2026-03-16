import pandas as pd


def derive_retido_bot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Define a coluna retido_bot para conversas do Ultimate.
    """

    df = df.copy()

    # garantir tipo datetime
    df["data_contato"] = pd.to_datetime(df["data_contato"], errors="coerce")

    # regra padrão
    df["retido_bot"] = df["conversation_status"].apply(
        lambda x: "sim" if x == "botHandled" else "não"
    )

    # período especial
    mask_periodo = (
        (df["data_contato"] >= pd.Timestamp("2026-02-19")) &
        (df["data_contato"] <= pd.Timestamp("2026-03-05"))
    )

    # regra do período
    mask_escalated = df["last_resolution"] != "escalatedAgent"

    df.loc[
        mask_periodo & mask_escalated,
        "retido_bot"
    ] = "sim"

    df.loc[
        mask_periodo & ~mask_escalated,
        "retido_bot"
    ] = "não"

    return df
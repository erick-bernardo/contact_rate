import pandas as pd


def enrich_marketplace(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enriquecimentos específicos para Zendesk Marketplace
    """

    df = df.copy()

    # origem
    df["origem_contato"] = "zendesk marketplace"

    # rename coluna
    df = df.rename(columns={
        "retido_bot_mkt": "retido_bot"
    })

    return df



def enrich_whatsapp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enriquecimentos específicos para Zendesk WhatsApp
    """

    df = df.copy()

    # origem
    df["origem_contato"] = "zendesk whatsapp"

    # remover prefixo do tag_pcid
    df["tag_pcid"] = (
        df["tag_pcid"]
        .astype("string")
        .str.replace("ultimate_pcid_", "", regex=False)
    )

    # criar flag retido_bot
    df["retido_bot"] = df["tag_pcid"].apply(
        lambda x: "não"
        if pd.notna(x) and str(x).strip() != ""
        else "não passou pelo bot"
    )

    # remover coluna antiga
    df = df.drop(columns=["retido_bot_mkt"])

    return df



def enrich_others(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enriquecimentos específicos para Zendesk Others
    """

    df = df.copy()

    # origem
    df["origem_contato"] = "zendesk others channels"

    # remover prefixo do tag_pcid
    df["tag_pcid"] = (
        df["tag_pcid"]
        .astype("string")
        .str.replace("ultimate_pcid_", "", regex=False)
    )

    # criar flag retido_bot
    df["retido_bot"] = df["tag_pcid"].apply(
        lambda x: "não"
        if pd.notna(x) and str(x).strip() != ""
        else "não passou pelo bot"
    )

    # remover coluna antiga
    df = df.drop(columns=["retido_bot_mkt"])

    return df
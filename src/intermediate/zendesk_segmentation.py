import pandas as pd


def zendesk_segmentation(df: pd.DataFrame):

    """
    Segmenta a base Zendesk em:

    - marketplace
    - whatsapp
    - others
    """

    df = df.copy()

    # =========================
    # Definições de canal
    # =========================

    whatsapp_channels = [
        "whatsapp"
    ]

    marketplace_channels = [
        "portal da amazon",
        "portal da b2w",
        "portal da cnova",
        "portal da leroy merlin",
        "portal da shopee",
        "portal da web continental",
        "portal do carrefour",
        "portal do intershop",
        "portal do magazine luiza",
        "portal do mercado livre mediação",
        "portal do mercado livre mensageria"
    ]

    # =========================
    # Segmentações
    # =========================

    zendesk_whatsapp = df[
        df["canal_atendimento"].isin(whatsapp_channels)
    ].copy()

    zendesk_marketplace = df[
        df["canal_atendimento"].isin(marketplace_channels)
    ].copy()

    zendesk_others = df[
        ~df["canal_atendimento"].isin(
            whatsapp_channels + marketplace_channels
        )
    ].copy()

    return zendesk_marketplace, zendesk_whatsapp, zendesk_others
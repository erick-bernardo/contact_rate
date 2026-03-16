import pandas as pd
from src.intermediate.schema_alignment import align_contact_schema

def build_operation_layer(
    zendesk_marketplace: pd.DataFrame,
    zendesk_others: pd.DataFrame,
    zendesk_whatsapp: pd.DataFrame
) -> pd.DataFrame:
    """
    Consolida as fontes de contato do Zendesk para a visão de Operação.
    Considera apenas contatos que NÃO foram retidos pelo bot.
    """

    # =========================
    # Filtro: retido_bot = não
    # =========================
    # Utilizamos .astype(str).str.lower() == 'não' para garantir que pegará
    # variações como 'Não', 'não' ou 'NÃO'. Adapte se o seu dado for booleano (False).
    
    mkt_filtered = zendesk_marketplace[zendesk_marketplace['retido_bot'].astype(str).str.lower() == 'não'].copy()
    others_filtered = zendesk_others[zendesk_others['retido_bot'].astype(str).str.lower() == 'não'].copy()
    wpp_filtered = zendesk_whatsapp[zendesk_whatsapp['retido_bot'].astype(str).str.lower() == 'não'].copy()

    # =========================
    # Alinhamento de schema
    # =========================
    mkt_aligned = align_contact_schema(mkt_filtered)
    others_aligned = align_contact_schema(others_filtered)
    wpp_aligned = align_contact_schema(wpp_filtered)

    # =========================
    # Concatenação
    # =========================
    df_operation = pd.concat(
        [
            mkt_aligned,
            others_aligned,
            wpp_aligned
        ],
        ignore_index=True
    )

    return df_operation
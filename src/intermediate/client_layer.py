import pandas as pd

from src.intermediate.schema_alignment import align_contact_schema


def build_client_layer(
    zendesk_marketplace,
    zendesk_others,
    mensageria,
    ultimate
) -> pd.DataFrame:

    """
    Consolida todas as fontes de contato do cliente.
    """

    # =========================
    # Alinhamento de schema
    # =========================

    zendesk_marketplace = align_contact_schema(zendesk_marketplace)
    zendesk_others = align_contact_schema(zendesk_others)
    mensageria = align_contact_schema(mensageria)
    ultimate = align_contact_schema(ultimate)

    # =========================
    # Concatenação
    # =========================

    df = pd.concat(
        [
            zendesk_marketplace,
            zendesk_others,
            mensageria,
            ultimate
        ],
        ignore_index=True
    )

    return df
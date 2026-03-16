import pandas as pd


CONTACT_SCHEMA = [
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
    "sku_item_quantidade",
    "origem_contato"
]


def align_contact_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Garante que o dataset tenha exatamente o schema padrão de contatos.
    """

    df = df.copy()

    # adicionar colunas faltantes
    for col in CONTACT_SCHEMA:
        if col not in df.columns:
            df[col] = pd.NA

    # remover colunas extras
    df = df[CONTACT_SCHEMA]

    return df
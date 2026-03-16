from src.validation.checks import *

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
    "retido_bot_mkt",
    "sku_item_quantidade",
    "origem_contato"
]


def validate_zendesk_stg(df):

    validate_schema(df, CONTACT_SCHEMA)

    check_duplicates(df, "id_contato")

    check_nulls(df, [
        "id_contato",
        "data_contato"
    ])

    check_volume(df, min_rows=400000)


def validate_mensageria_stg(df):

    print("\n=== Validando STG Mensageria ===")

    validate_schema(df, CONTACT_SCHEMA)

    check_duplicates(df, ["id_contato", "id_pedido"])

    check_nulls(df, [
        "id_contato",
        "data_contato",
        "id_pedido"
    ])

    check_volume(df, min_rows=10)

    print("✔ Validação mensageria concluída")   


def validate_ultimate_stg(df):

    print("\n=== Validando STG Ultimate ===")

    validate_schema(df, CONTACT_SCHEMA)

    check_duplicates(df, ["id_contato", "id_pedido"])

    check_nulls(df, [
        "id_contato",
        "data_contato",
        "id_pedido"
    ])

    check_volume(df, min_rows=10)

    print("✔ Validação ultimate concluída")   
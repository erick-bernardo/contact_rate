import pandas as pd
from src.intermediate.ultimate_enrichment import derive_retido_bot


def stg_ultimate(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    # =========================
    # Rename de colunas
    # =========================

    df = df.rename(columns={
        "conversation_id": "id_contato",
        "conversation_start_time_br": "data_contato",
        "id_pedido_filho": "id_pedido",
        "platform_conversation_id": "tag_pcid",
        "data_use_case": "motivo_principal"
    })

    # =========================
    # Colunas fixas
    # =========================

    df["canal_atendimento"] = "whatsapp"
    df["submotivo_principal"] = "bot_ultimate_mm_wtts"

    # =========================
    # Colunas inexistentes
    # =========================

    df["seller"] = pd.NA
    df["tipo_ticket"] = pd.NA
    df["status_ticket"] = pd.NA
    df["via_channel"] = pd.NA
    df["sku_item_quantidade"] = pd.NA

    # =========================
    # De-para formulário
    # =========================

    df["motivo_principal"] = df["motivo_principal"].replace({
        "speak to attendant": "speak to attendant - select order"
    })

    formulario_map = {
        "order cancelation": "cancelamento e devolução",
        "order cancelation - select order": "cancelamento e devolução",
        "order status": "entrega",
        "delayed delivery": "entrega",
        "order tracking link": "entrega",
        "order invoice": "entrega",
        "product help": "produto",
        "product services": "indefinido",
        "sem use case": "indefinido",
        "user greetings": "indefinido",
        "speak to attendant": "indefinido",
        "speak to attendant - select order": "indefinido",
        "proactive": "indefinido"
    }

    df["formulario"] = df["motivo_principal"].map(formulario_map)

    df["formulario"] = df["formulario"].fillna("indefinido")


    # Regra de retenção BOT
    df = derive_retido_bot(df)


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
        "sku_item_quantidade",
        # Colunas de vendas (agora vêm no raw)
        "nome_produto",
        "marca",
        "categoria_produto",
        "head_categoria",
        "empresa_venda",
        "tipo_venda",
        "status_pedido",
        "nome_fornecedor",
        "nome_transportadora",
        "data_entrega_cliente_revisada",
        "data_compra_cliente",
        "data_entregue",
        "data_entrega",
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

    # Tratamento de datas de vendas
    date_cols_vendas = [
        "data_entrega_cliente_revisada",
        "data_compra_cliente",
        "data_entregue",
        "data_entrega",
        "data_cancelamento",
        "data_aprovacao",
        "data_entregue_cliente",
        "data_prometido_entrega_cliente"
    ]

    for col in date_cols_vendas:
        df[col] = pd.to_datetime(
            df[col],
            errors="coerce"
        )

    # Tratamento de id_cliente (remover decimais)
    df["id_cliente"] = (
        df["id_cliente"]
        .astype("string")
        .str.replace(r'\D+', '', regex=True)
        .str.strip()
        .replace('', pd.NA)
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
        "retido_bot",
        # Colunas de vendas
        "nome_produto",
        "marca",
        "categoria_produto",
        "head_categoria",
        "empresa_venda",
        "tipo_venda",
        "status_pedido",
        "nome_fornecedor",
        "nome_transportadora",
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

    df["origem_contato"] = "bot ultimate whatsapp"
    df["origem_contato"] = df["origem_contato"].astype("string")

    

    return df
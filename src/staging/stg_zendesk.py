import pandas as pd
import numpy as np

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
        errors="coerce",
        utc=False
    )

    df["id_pedido_pai"] = (
        df["id_pedido_pai"]
        .astype("string")
        .str.replace(r'\D+', '', regex=True)
        .str.strip()
        .replace('', np.nan)
    )

    df["id_pedido"] = (
        df["id_pedido"]
        .astype("string")
        .str.replace(r'\D+', '', regex=True)
        .str.strip()
        .replace('', np.nan)
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
        .replace('', np.nan)
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
        "retido_bot_mkt",
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
    
    # =========================
    # Coluna origem
    # =========================

    df["origem_contato"] = "zendesk"
    df["origem_contato"] = df["origem_contato"].astype("string")

    return df
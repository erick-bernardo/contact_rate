import pandas as pd


def _apply_common_zendesk_filters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Regras comuns de contatos válidos para Zendesk.
    Aplicadas a todos os canais.
    """

    df = df.copy()

    before = len(df)

    # filtro formulário
    df = df[df["formulario"] != "formulário de ticket padrão"]

    # filtro via channel
    df = df[df["via_channel"] != "side_conversation"]

    after = len(df)

    print(f"Regra comum Zendesk removeu {before - after} registros")

    return df


def valid_contacts_marketplace(df: pd.DataFrame) -> pd.DataFrame:
    """
    Contatos válidos para canais de marketplace.
    """

    df = _apply_common_zendesk_filters(df)
    
    # regras específicas de marketplace podem entrar aqui

    return df


def valid_contacts_whatsapp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Contatos válidos para canais de WhatsApp.
    """

    df = _apply_common_zendesk_filters(df)

    # regras específicas de whatsapp podem entrar aqui
    df = df[df["tipo_ticket"] != "task"]


    return df


def valid_contacts_others(df: pd.DataFrame) -> pd.DataFrame:
    """
    Contatos válidos para outros canais.
    """

    df = _apply_common_zendesk_filters(df)

    # regras específicas de outros canais podem entrar aqui
    df = df[df["tipo_ticket"] != "task"]


    return df
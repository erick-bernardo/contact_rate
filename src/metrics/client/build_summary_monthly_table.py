import pandas as pd

def build_summary_monthly_table(df_agrouped_client: pd.DataFrame) -> pd.DataFrame:
    df = df_agrouped_client.copy()
    
    # Padronizar texto para evitar erros de case sensitive nos filtros
    df['tipo_venda'] = df['tipo_venda'].fillna('null').astype(str).str.lower()
    df['origem_contato'] = df['origem_contato'].fillna('').astype(str).str.lower()
    df['retido_bot'] = df['retido_bot'].fillna('').astype(str).str.lower()

    # Função super simples que apenas filtra e conta IDs únicos agrupados por mês
    def calc_mes(mask=None):
        base = df if mask is None else df[mask]
        return base.groupby('mes_contato')['id_contato'].nunique()

    # Dicionário direto: As chaves são tuplas de ("regra", "indicador") 
    # e os valores são os cálculos diretos no dataframe.
    calculos = {
        # TOTAL GERAL
        ("total", "geral"): calc_mes(),
        
        # 1P
        ("1p", "Cliente 1p"): calc_mes(df['tipo_venda'] == '1p'),
        ("1p bot ultimate whatsapp", "Whatsapp"): calc_mes((df['tipo_venda'] == '1p') & (df['origem_contato'] == 'bot ultimate whatsapp')),
        ("1p bot ultimate whatsapp sim", "Retido"): calc_mes((df['tipo_venda'] == '1p') & (df['origem_contato'] == 'bot ultimate whatsapp') & (df['retido_bot'] == 'sim')),
        ("1p bot ultimate whatsapp não", "Não retido"): calc_mes((df['tipo_venda'] == '1p') & (df['origem_contato'] == 'bot ultimate whatsapp') & (df['retido_bot'] == 'não')),
        
        ("1p zendesk marketplace", "Portais Marketplace"): calc_mes((df['tipo_venda'] == '1p') & (df['origem_contato'] == 'zendesk marketplace')),
        ("1p zendesk marketplace sim", "Retido"): calc_mes((df['tipo_venda'] == '1p') & (df['origem_contato'] == 'zendesk marketplace') & (df['retido_bot'] == 'sim')),
        ("1p zendesk marketplace não", "Não retido"): calc_mes((df['tipo_venda'] == '1p') & (df['origem_contato'] == 'zendesk marketplace') & (df['retido_bot'] == 'não')),
        
        ("1p zendesk others channels", "Demais canais"): calc_mes((df['tipo_venda'] == '1p') & (df['origem_contato'] == 'zendesk others channels')),

        # 3P
        ("3p", "3P"): calc_mes(df['tipo_venda'] == '3p'),
        ("3p bot ultimate whatsapp", "Whatsapp"): calc_mes((df['tipo_venda'] == '3p') & (df['origem_contato'] == 'bot ultimate whatsapp')),
        ("3p bot ultimate whatsapp sim", "Retido"): calc_mes((df['tipo_venda'] == '3p') & (df['origem_contato'] == 'bot ultimate whatsapp') & (df['retido_bot'] == 'sim')),
        ("3p bot ultimate whatsapp não", "Não retido"): calc_mes((df['tipo_venda'] == '3p') & (df['origem_contato'] == 'bot ultimate whatsapp') & (df['retido_bot'] == 'não')),
        
        ("3p zendesk others channels", "Demais canais"): calc_mes((df['tipo_venda'] == '3p') & (df['origem_contato'] == 'zendesk others channels')),

        # MENSAGERIA SELLER
        ("mensageria seller", "Mensageria Seller"): calc_mes(df['origem_contato'] == 'mensageria seller'),

        # NÃO MAPEADO
        ("tipo_venda = null", "Não mapeado"): calc_mes(df['tipo_venda'] == 'null'),
        ("tipo_venda = null e retido_bot = sim", "Retido"): calc_mes((df['tipo_venda'] == 'null') & (df['retido_bot'] == 'sim')),
        ("tipo_venda = null e retido_bot = não", "Não retido"): calc_mes((df['tipo_venda'] == 'null') & (df['retido_bot'] == 'não')),

        # VOLUME RECONTATO
         ("recontato", "Recontato"): calc_mes(df['flag_recontato'] == 'sim'),

    }

    # Transforma o dicionário em DataFrame e transpõe (.T)
    df_final = pd.DataFrame(calculos).T
    
    # Substitui os nulos (meses onde não houve contato para aquela regra) por 0 e converte para inteiro
    df_final = df_final.fillna(0).astype(int)
    
    # Reseta o index para que as tuplas ("regra", "indicador") voltem a ser colunas do dataframe
    df_final.index.names = ['regra', 'indicador']
    df_final = df_final.reset_index()

    return df_final
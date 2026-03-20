import pandas as pd

def build_op_summary_monthly_table(df_agrouped_operation: pd.DataFrame) -> pd.DataFrame:
    df = df_agrouped_operation.copy()
    
    # Padronizar texto para evitar erros de case sensitive nos filtros
    df['tipo_venda'] = df['tipo_venda'].fillna('null').astype(str).str.lower()
    df['origem_contato'] = df['origem_contato'].fillna('').astype(str).str.lower()
    df['retido_bot'] = df['retido_bot'].fillna('').astype(str).str.lower()

    # Função super simples que apenas filtra e conta IDs únicos agrupados por mês
    def calc_mes(mask=None):
        base = df if mask is None else df[mask]
        return base.groupby('mes_contato')['id_pedido'].nunique()

    # Dicionário direto: As chaves são tuplas de ("regra", "indicador") 
    # e os valores são os cálculos diretos no dataframe.
    calculos = {
        # TOTAL GERAL
        ("total", "geral"): calc_mes(),
        
        # 1P
        ("1p", "Operação 1p"): calc_mes(df['tipo_venda'] == '1p'),
        ("1p zendesk whatsapp não", "whatsapp"): calc_mes((df['tipo_venda'] == '1p') & (df['origem_contato'] == 'zendesk whatsapp')),
        ("1p zendesk marketplace não", "marketplace"): calc_mes((df['tipo_venda'] == '1p') & (df['origem_contato'] == 'zendesk marketplace')),
        ("1p zendesk others channels", "Demais canais"): calc_mes((df['tipo_venda'] == '1p') & (df['origem_contato'] == 'zendesk others channels')),

        # 3P
        ("3p", "3P"): calc_mes(df['tipo_venda'] == '3p'),
        ("3p zendesk whatsapp não", "whatsapp"): calc_mes((df['tipo_venda'] == '3p') & (df['origem_contato'] == 'zendesk whatsapp')),
        ("3p zendesk others channels", "Demais canais"): calc_mes((df['tipo_venda'] == '3p') & (df['origem_contato'] == 'zendesk others channels')),

        # NÃO MAPEADO
        ("tipo_venda = null e retido_bot = não", "Não mapeado"): calc_mes((df['tipo_venda'] == 'null')),

        # VOLUME RECONTATO
        ("recontato", "Recontato"): calc_mes(df['flag_recontato'] == 'sim'),

        # JORNADA 1P 
        ("1p ", "entrega"): calc_mes((df['tipo_venda'] == '1p') & (df['jornada_atendimento'] == 'entrega')),
        ("1p ", "cancelamento e devolução"): calc_mes((df['tipo_venda'] == '1p') & (df['jornada_atendimento'] == 'cancelamento e devolução')),
        ("1p ", "produto"): calc_mes((df['tipo_venda'] == '1p') & (df['jornada_atendimento'] == 'produto')),
        ("1p ", "demais jornadas"): calc_mes((df['tipo_venda'] == '1p') & (df['jornada_atendimento'] == 'demais jornadas')),

        # JORNADA 3P 
        ("3p ", "entrega"): calc_mes((df['tipo_venda'] == '3p') & (df['jornada_atendimento'] == 'entrega')),
        ("3p ", "cancelamento e devolução"): calc_mes((df['tipo_venda'] == '3p') & (df['jornada_atendimento'] == 'cancelamento e devolução')),
        ("3p ", "produto"): calc_mes((df['tipo_venda'] == '3p') & (df['jornada_atendimento'] == 'produto')),
        ("3p ", "demais jornadas"): calc_mes((df['tipo_venda'] == '3p') & (df['jornada_atendimento'] == 'demais jornadas')),

    }

    # Transforma o dicionário em DataFrame e transpõe (.T)
    df_final = pd.DataFrame(calculos).T
    
    # Substitui os nulos (meses onde não houve contato para aquela regra) por 0 e converte para inteiro
    df_final = df_final.fillna(0).astype(int)
    
    # Reseta o index para que as tuplas ("regra", "indicador") voltem a ser colunas do dataframe
    df_final.index.names = ['regra', 'indicador']
    df_final = df_final.reset_index()

    return df_final
import pandas as pd
import numpy as np

def build_client_layer_metrics(
    df_client: pd.DataFrame, 
    df_sales: pd.DataFrame, 
    period_col_client: str, 
    period_col_sales: str
) -> pd.DataFrame:
    """
    Gera a visão tabular de indicadores da Camada Cliente (CR, Volumes e Derivação).
    Agrupa dinamicamente pelo período informado (dia, semana ou mês).
    """
    # 1. Copiar bases e padronizar o nome da coluna de período para o agrupamento interno
    c = df_client.rename(columns={period_col_client: 'periodo'}).copy()
    s = df_sales.rename(columns={period_col_sales: 'periodo'}).copy()
    
    # 2. Padronização de strings para evitar quebra de filtros
    cols_client = ['tipo_venda', 'jornada_atendimento', 'grupo_venda', 'retido_bot', 'origem_contato', 'flag_recontato']
    for col in cols_client:
        if col in c.columns:
            c[col] = c[col].fillna('null').astype(str).str.lower()
            
    cols_sales = ['tipo_venda', 'grupo_venda']
    for col in cols_sales:
        if col in s.columns:
            s[col] = s[col].fillna('null').astype(str).str.lower()

    # 3. Funções auxiliares de agregação usando a coluna genérica 'periodo'
    def calc_contatos(mask=None):
        base = c if mask is None else c[mask]
        return base.groupby('periodo')['id_contato'].nunique()

    def calc_vendas(mask=None):
        base = s if mask is None else s[mask]
        return base.groupby('periodo')['itens_vendidos'].sum()

    def div_taxa(num, den):
        idx = num.index.union(den.index)
        n = num.reindex(idx).fillna(0)
        d = den.reindex(idx).replace(0, pd.NA)
        return (n / d).fillna(0)

    # ==========================================
    # BASES COMUNS
    # ==========================================
    vol_s_total = calc_vendas()
    vol_s_1p = calc_vendas(s['tipo_venda'] == '1p')
    vol_s_3p = calc_vendas(s['tipo_venda'] == '3p')
    vol_s_mkt = calc_vendas(s['grupo_venda'] == 'marketplaces')

    vol_c_total = calc_contatos()
    vol_c_1p = calc_contatos(c['tipo_venda'] == '1p')
    vol_c_3p = calc_contatos(c['tipo_venda'] == '3p')
    vol_c_mkt = calc_contatos(c['grupo_venda'] == 'marketplaces')
    
    vol_derivados = calc_contatos((c['retido_bot'] != 'sim') & (c['origem_contato'] != 'mensageria seller'))
    mask_demais = c['jornada_atendimento'].isin(['demais jornadas', 'demais formulários'])

    # ==========================================
    # DICIONÁRIO ORDENADO DAS MÉTRICAS
    # ==========================================
    linhas = [
        ("Contact Rate", pd.Series(dtype=float), 'vazio'), 
        ("CR - Contact Rate | Cliente", div_taxa(vol_c_total, vol_s_total), 'pct'),
        ("CR - Recontato", div_taxa(calc_contatos(c['flag_recontato'] == 'sim'), vol_c_total), 'pct'),
        ("", pd.Series(dtype=float), 'vazio'), 
        
        ("Contatos - Total Contatos | Cliente", vol_c_total, 'vol'),
        ("Contatos - Entrega", calc_contatos(c['jornada_atendimento'] == 'entrega'), 'vol'),
        ("Contatos - Cancelamento e devolução", calc_contatos(c['jornada_atendimento'] == 'cancelamento e devolução'), 'vol'),
        ("Contatos - Produto", calc_contatos(c['jornada_atendimento'] == 'produto'), 'vol'),
        ("Contatos - Demais jornadas", calc_contatos(mask_demais), 'vol'),
        
        ("Contatos - Total 1P", vol_c_1p, 'vol'),
        ("Contatos - 1P | Entrega", calc_contatos((c['tipo_venda'] == '1p') & (c['jornada_atendimento'] == 'entrega')), 'vol'),
        ("Contatos - 1P | Cancelamento e devolução", calc_contatos((c['tipo_venda'] == '1p') & (c['jornada_atendimento'] == 'cancelamento e devolução')), 'vol'),
        ("Contatos - 1P | Produto", calc_contatos((c['tipo_venda'] == '1p') & (c['jornada_atendimento'] == 'produto')), 'vol'),
        ("Contatos - 1P | Demais jornadas", calc_contatos((c['tipo_venda'] == '1p') & mask_demais), 'vol'),
        
        ("Contatos - Total 3P", vol_c_3p, 'vol'),
        ("Contatos - 3P | Entrega", calc_contatos((c['tipo_venda'] == '3p') & (c['jornada_atendimento'] == 'entrega')), 'vol'),
        ("Contatos - 3P | Cancelamento e devolução", calc_contatos((c['tipo_venda'] == '3p') & (c['jornada_atendimento'] == 'cancelamento e devolução')), 'vol'),
        ("Contatos - 3P | Produto", calc_contatos((c['tipo_venda'] == '3p') & (c['jornada_atendimento'] == 'produto')), 'vol'),
        ("Contatos - 3P | Demais jornadas", calc_contatos((c['tipo_venda'] == '3p') & mask_demais), 'vol'),
        
        ("Contatos - Não mapeados", calc_contatos(c['tipo_venda'] == 'null'), 'vol'),
        
        ("CR - Contact Rate 1P", div_taxa(vol_c_1p, vol_s_1p), 'pct'),
        ("CR - 1P | Entrega", div_taxa(calc_contatos((c['tipo_venda'] == '1p') & (c['jornada_atendimento'] == 'entrega')), vol_s_1p), 'pct'),
        ("CR - 1P | Cancelamento e devolução", div_taxa(calc_contatos((c['tipo_venda'] == '1p') & (c['jornada_atendimento'] == 'cancelamento e devolução')), vol_s_1p), 'pct'),
        ("CR - 1P | Produto", div_taxa(calc_contatos((c['tipo_venda'] == '1p') & (c['jornada_atendimento'] == 'produto')), vol_s_1p), 'pct'),
        ("CR - 1P | Demais jornadas", div_taxa(calc_contatos((c['tipo_venda'] == '1p') & mask_demais), vol_s_1p), 'pct'),
        
        ("CR - Marketplaces", div_taxa(vol_c_mkt, vol_s_mkt), 'pct'),
        ("Contatos - Marketplaces", vol_c_mkt, 'vol'),
        
        ("CR - Contact Rate 3P", div_taxa(vol_c_3p, vol_s_3p), 'pct'),
        ("CR - 3P | Entrega", div_taxa(calc_contatos((c['tipo_venda'] == '3p') & (c['jornada_atendimento'] == 'entrega')), vol_s_3p), 'pct'),
        ("CR - 3P | Cancelamento e devolução", div_taxa(calc_contatos((c['tipo_venda'] == '3p') & (c['jornada_atendimento'] == 'cancelamento e devolução')), vol_s_3p), 'pct'),
        ("CR - 3P | Produto", div_taxa(calc_contatos((c['tipo_venda'] == '3p') & (c['jornada_atendimento'] == 'produto')), vol_s_3p), 'pct'),
        ("CR - 3P | Demais jornadas", div_taxa(calc_contatos((c['tipo_venda'] == '3p') & mask_demais), vol_s_3p), 'pct'),
        
        ("CR - 3P | Mensageria Seller", div_taxa(calc_contatos((c['tipo_venda'] == '3p') & (c['origem_contato'] == 'mensageria seller')), vol_s_3p), 'pct'),
        
        ("Contatos - Derivados para operação", vol_derivados, 'vol'),
        ("CR - % Derivados para operação", div_taxa(vol_derivados, vol_c_total), 'pct'),
        ("", pd.Series(dtype=float), 'vazio'), 
        
        ("Vendas - Total Vendas", vol_s_total, 'vol'),
        ("Vendas - Vendas 1P", vol_s_1p, 'vol'),
        ("Vendas - Vendas 3P", vol_s_3p, 'vol'),
    ]

    # ==========================================
    # CONSTRUÇÃO DO DATAFRAME FINAL E FORMATAÇÃO
    # ==========================================
    # Estrutura: linhas = indicadores, colunas = períodos
    # Criar dicionário com os dados
    dados = {}
    for nome, serie, formato in linhas:
        dados[nome] = (serie, formato)
    
    # Extrair todos os períodos únicos
    todos_periodos = set()
    for serie, _ in dados.values():
        todos_periodos.update(serie.index)
    periodos_ordenados = sorted(todos_periodos)
    
    # Construir DataFrame: linhas = indicadores, colunas = períodos
    df_final = pd.DataFrame(index=[nome for nome, _, _ in linhas], columns=periodos_ordenados)
    
    # Preencher valores com formato correto
    for nome, (serie, formato) in dados.items():
        for periodo in periodos_ordenados:
            if periodo in serie.index:
                val = serie[periodo]
            else:
                val = np.nan
            
            if formato == 'vazio' or pd.isna(val):
                df_final.at[nome, periodo] = ""
            elif formato == 'pct':
                try:
                    df_final.at[nome, periodo] = f"{(float(val) * 100):.2f}%".replace('.', ',')
                except (ValueError, TypeError):
                    df_final.at[nome, periodo] = val
            elif formato == 'vol':
                try:
                    df_final.at[nome, periodo] = int(float(val))
                except (ValueError, TypeError):
                    df_final.at[nome, periodo] = val
    
    # Reset index para colocar indicadores como coluna
    df_final.reset_index(inplace=True)
    df_final.rename(columns={'index': 'Indicador'}, inplace=True)
                
    return df_final
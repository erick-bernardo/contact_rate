from src.extract.redshift_extractor import redshift_weekly_extractor

from src.config.settings import RAW_MENSAGERIA_PATH


def extract_mensageria(bootstrap=False):
    """
    Extrai dados de contatos Mensageria com vendas já enriquecidas.
    
    Query: contatos_mensageria.sql
    Responsável: Redshift (ultimate_mm_whatsapp + looker_sales com JOIN)
    
    Os dados chegam aqui já com:
    - 17 colunas de vendas
    - 8 datas de vendas parseadas
    - IDs limpos via REGEXP_REPLACE
    
    Args:
        bootstrap (bool): Se True, processa últimos 90 dias. 
                         Se False, processa últimos 7 dias + semana atual.
    """
    
    redshift_weekly_extractor(
        query_name="contatos_mensageria.sql",
        date_column="data_atendimento",
        raw_path=RAW_MENSAGERIA_PATH,
        bootstrap=bootstrap
    )

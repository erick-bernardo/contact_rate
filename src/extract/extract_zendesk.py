from src.extract.redshift_extractor import redshift_weekly_extractor

from src.config.settings import RAW_ZENDESK_PATH


def extract_zendesk(bootstrap=False):
    """
    Extrai dados de tickets Zendesk com vendas já enriquecidas.
    
    Query: tickets_gold_zendesk_v2.sql
    Responsável: Redshift (zendesk_tickets_gold + looker_sales com JOIN)
    
    Os dados chegam aqui já com:
    - 17 colunas de vendas
    - 8 datas de vendas parseadas
    - IDs limpos via REGEXP_REPLACE
    
    Args:
        bootstrap (bool): Se True, processa últimos 90 dias. 
                         Se False, processa últimos 7 dias + semana atual.
    """
    
    redshift_weekly_extractor(
        query_name="tickets_gold_zendesk_v2.sql",
        date_column="data_atendimento",
        raw_path=RAW_ZENDESK_PATH,
        bootstrap=bootstrap
    )
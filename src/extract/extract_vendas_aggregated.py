import pandas as pd
from src.extract.redshift_extractor import redshift_weekly_extractor

from datetime import datetime, timedelta

from src.config.settings import (
    RAW_VENDAS_PATH,
    DELTA_DAYS_RAW_EXTRACTION
)

#from src.utils.logger import setup_logger

#logger = setup_logger()


def extract_vendas_aggregated(bootstrap=False):
    """
    Extrai base agregada semanal de vendas do Redshift.
    
    Query: vendas_base_semanal.sql
    Fonte: looker_plan.looker_sales (com filtros e agregações)
    
    Propósito: 
    - Substitui a anterior raw/vendas que continha dados brutos completos
    - Base AGREGADA por semana, não mais dados brutos
    - Utilizada para validações e métricas secundárias
    
    Processamento:
    - Bootstrap: Processa últimos 90 dias agregados
    - Normal: Processa últimos 7 dias + semana atual
    
    Salvamento: Por semana em format ISO 8601 (YYYY-Www.parquet)
    
    Args:
        bootstrap (bool): Se True, processa últimos 90 dias. 
                         Se False, processa últimos 7 dias + semana atual.
    """

    #logger.info("===== EXTRAÇÃO VENDAS AGGREGATED START =====")

    redshift_weekly_extractor(
        query_name="vendas_base_semanal.sql",
        date_column="data_venda",
        raw_path=RAW_VENDAS_PATH,
        bootstrap=bootstrap
    )
    
    #logger.info("===== EXTRAÇÃO VENDAS AGGREGATED END =====")

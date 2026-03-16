from src.extract.redshift_extractor import redshift_weekly_extractor

from src.config.settings import RAW_VENDAS_PATH


def extract_auxiliar_vendas(bootstrap=False):

    redshift_weekly_extractor(
        query_name="auxiliar_vendas.sql",
        date_column="data_compra_cliente",
        raw_path=RAW_VENDAS_PATH,
        bootstrap=bootstrap
    )
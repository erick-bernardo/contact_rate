from src.extract.redshift_extractor import redshift_weekly_extractor

from src.config.settings import RAW_MENSAGERIA_PATH


def extract_mensageria(bootstrap=False):
    
    redshift_weekly_extractor(
        query_name="contatos_mensageria.sql",
        date_column="data_atendimento",
        raw_path=RAW_MENSAGERIA_PATH,
        bootstrap=bootstrap
    )

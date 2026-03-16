from src.extract.redshift_extractor import redshift_weekly_extractor

from src.config.settings import RAW_ZENDESK_PATH


def extract_zendesk(bootstrap=False):
    
    redshift_weekly_extractor(
        query_name="tickets_gold_zendesk.sql",
        date_column="created_at",
        raw_path=RAW_ZENDESK_PATH,
        bootstrap=bootstrap
    )
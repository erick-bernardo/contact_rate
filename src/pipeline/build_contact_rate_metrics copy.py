import pandas as pd

from src.utils.logger import setup_logger




logger = setup_logger()


def build_contact_rate_metrics():

    logger.info("===== CONTACT RATE PIPELINE START =====")

    # -----------------------------------------
    # 1 EXTRAÇÃO RAW
    # -----------------------------------------

    logger.info("Etapa 1 - Extração RAW")

    from src.loaders.load_raw_parquet import load_raw_dataset

    from src.config.settings import (
        RAW_ZENDESK_PATH,
        RAW_MENSAGERIA_PATH,
        RAW_ULTIMATE_PATH,
        RAW_VENDAS_PATH
    )

    logger.info("Carregando datasets RAW")

    zendesk_raw = load_raw_dataset(RAW_ZENDESK_PATH)
    mensageria_raw = load_raw_dataset(RAW_MENSAGERIA_PATH)
    ultimate_raw = load_raw_dataset(RAW_ULTIMATE_PATH)
    vendas_raw = load_raw_dataset(RAW_VENDAS_PATH)


    # será conectado depois
    # extract_auxiliar_vendas()
    # extract_zendesk()
    # extract_mensageria()
    # extract_ultimate_from_csv()

    # -----------------------------------------
    # 2 LOAD RAW
    # -----------------------------------------

    logger.info("Etapa 2 - Load RAW")

    # carregar raw parquet

    # -----------------------------------------
    # 3 STAGING
    # -----------------------------------------

    logger.info("Etapa 3 - Staging")

    # stg_zendesk
    # stg_mensageria
    # stg_ultimate
    # stg_vendas

    # -----------------------------------------
    # 4 INTERMEDIATE
    # -----------------------------------------

    logger.info("Etapa 4 - Intermediate")

    # segmentação zendesk
    # validação contatos

    # -----------------------------------------
    # 5 CLIENT LAYER
    # -----------------------------------------

    logger.info("Etapa 5 - Client layer")

    # build_client_layer()

    # -----------------------------------------
    # 6 GLOBAL ENRICHMENT
    # -----------------------------------------

    logger.info("Etapa 6 - Global enrichment")

    # add_time_features
    # join_sales_data
    # recontato
    # jornada

    # -----------------------------------------
    # 7 MÉTRICAS BASE
    # -----------------------------------------

    logger.info("Etapa 7 - Base metrics")

    # generate_base_metrics()

    # -----------------------------------------
    # 8 MÉTRICAS FINAIS
    # -----------------------------------------

    logger.info("Etapa 8 - Metric layer")

    # apply_metric_layer()

    logger.info("===== CONTACT RATE PIPELINE END =====")
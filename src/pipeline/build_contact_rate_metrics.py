import pandas as pd

from src.utils.logger import setup_logger

# ===============================
# Configurações
# ===============================

from src.config.settings import (
    PROJECT_ROOT,
    DATA_PATH,
    RAW_PATH,
    STAGED_PATH,
    MART_PATH,
    METRICS_PATH,
    RAW_ZENDESK_PATH,
    RAW_MENSAGERIA_PATH,
    RAW_ULTIMATE_PATH,
    RAW_VENDAS_PATH,
    CLIENT_CONTACT_RATE_PATH,
    OPERATION_CONTACT_RATE_PATH,
    #METRIC_BASE_PATH,
    #METRIC_FINAL_PATH,
    CLIENT_METRIC_CUBE_BASE,
    CLIENT_METRIC_CUBE_FINAL,
    OPERATION_METRIC_CUBE_BASE, 
    OPERATION_METRIC_CUBE_FINAL,
    SAMPLE_PATH
)

# ===============================
# Loaders
# ===============================

from src.load.load_raw_parquet import load_raw_dataset

# ===============================
# STAGING
# ===============================

from src.staging.stg_zendesk import stg_zendesk
from src.staging.stg_mensageria import stg_mensageria
from src.staging.stg_ultimate import stg_ultimate
from src.staging.stg_vendas import stg_vendas
from src.staging.stg_vendas_cube import stg_vendas_cube

# ===============================
# INTERMEDIATE
# ===============================

from src.intermediate.zendesk_segmentation import zendesk_segmentation

from src.intermediate.zendesk_contact_filters import (
    valid_contacts_marketplace,
    valid_contacts_whatsapp,
    valid_contacts_others
)

from src.intermediate.zendesk_channel_enrichments import (
    enrich_marketplace,
    enrich_whatsapp,
    enrich_others
)

# ===============================
# MART LAYER
# ===============================

from src.mart.client_contact_rate import build_client_layer
#from src.mart.operation_contact_rate import build_operation_layer


# ===============================
# ENRICHMENTS
# ===============================

from src.enrichment.global_enrichment import global_enrichment

# ===============================
# METRICS
# ===============================

from src.metrics.generate_base_metrics import generate_base_metrics
from src.metrics.apply_metric_layer import apply_metric_layer


logger = setup_logger()


def build_contact_rate_metrics():

    logger.info("========================================")
    logger.info("CONTACT RATE PIPELINE START")
    logger.info("========================================")

    # ======================================================
    # 1 LOAD RAW DATASETS
    # ======================================================

    logger.info("Etapa 1 - Carregando datasets RAW")

    zendesk_raw = load_raw_dataset(RAW_ZENDESK_PATH)
    mensageria_raw = load_raw_dataset(RAW_MENSAGERIA_PATH)
    ultimate_raw = load_raw_dataset(RAW_ULTIMATE_PATH)
    vendas_raw = load_raw_dataset(RAW_VENDAS_PATH)

    logger.info(f"Zendesk RAW: {len(zendesk_raw)} linhas")
    logger.info(f"Mensageria RAW: {len(mensageria_raw)} linhas")
    logger.info(f"Ultimate RAW: {len(ultimate_raw)} linhas")
    logger.info(f"Vendas RAW: {len(vendas_raw)} linhas")

    # ======================================================
    # 2 STAGING
    # ======================================================

    logger.info("Etapa 2 - Staging")

    zendesk_stg = stg_zendesk(zendesk_raw)
    mensageria_stg = stg_mensageria(mensageria_raw)
    ultimate_stg = stg_ultimate(ultimate_raw)
    vendas_stg = stg_vendas(vendas_raw)

    logger.info(f"Zendesk STG: {len(zendesk_stg)} linhas")
    logger.info(f"Mensageria STG: {len(mensageria_stg)} linhas")
    logger.info(f"Ultimate STG: {len(ultimate_stg)} linhas")
    logger.info(f"Vendas STG: {len(vendas_stg)} linhas")

    # ======================================================
    # 3 SEGMENTAÇÃO ZENDESK
    # ======================================================

    logger.info("Etapa 3 - Segmentação Zendesk")

    zendesk_marketplace, zendesk_whatsapp, zendesk_others = zendesk_segmentation(
        zendesk_stg
    )

    logger.info(f"Marketplace: {len(zendesk_marketplace)}")
    logger.info(f"Whatsapp: {len(zendesk_whatsapp)}")
    logger.info(f"Others: {len(zendesk_others)}")

    # ======================================================
    # 4 VALIDAÇÃO DE CONTATOS
    # ======================================================

    logger.info("Etapa 4 - Aplicando regras de contatos válidos")

    marketplace_valid_df = marketplace_valid(zendesk_marketplace)
    whatsapp_valid_df = whatsapp_valid(zendesk_whatsapp)
    others_valid_df = others_valid(zendesk_others)

    logger.info(f"Marketplace valid: {len(marketplace_valid_df)}")
    logger.info(f"Whatsapp valid: {len(whatsapp_valid_df)}")
    logger.info(f"Others valid: {len(others_valid_df)}")

    # ======================================================
    # 5 CLIENT LAYER
    # ======================================================

    logger.info("Etapa 5 - Construindo client_contact_rate")

    client_df = build_client_layer(

        marketplace_valid_df,
        others_valid_df,
        mensageria_stg,
        ultimate_stg
    )

    logger.info(f"Client layer: {len(client_df)} linhas")

    # ======================================================
    # 6 GLOBAL ENRICHMENT
    # ======================================================

    logger.info("Etapa 6 - Enriquecimentos globais")

    client_enriched = global_enrichment(

        client_df,
        vendas_stg

    )

    logger.info(f"Client enriched: {len(client_enriched)} linhas")

    # ======================================================
    # 7 SALVAR CLIENT CONTACT RATE
    # ======================================================

    logger.info("Salvando client_contact_rate")

    CLIENT_CONTACT_RATE_PATH.mkdir(parents=True, exist_ok=True)

    client_path = CLIENT_CONTACT_RATE_PATH / "client_contact_rate.parquet"

    client_enriched.to_parquet(

        client_path,
        index=False

    )

    logger.info(f"Arquivo salvo: {client_path}")

    # ======================================================
    # 8 GERAR MÉTRICAS BASE
    # ======================================================

    logger.info("Etapa 7 - Gerando métricas base")

    metric_cube_base = generate_base_metrics(

        client_enriched

    )

    METRIC_BASE_PATH.mkdir(parents=True, exist_ok=True)

    metric_base_path = METRIC_BASE_PATH / "metric_cube_base.parquet"

    metric_cube_base.to_parquet(

        metric_base_path,
        index=False

    )

    logger.info(f"Metric cube base salvo: {metric_base_path}")

    # ======================================================
    # 9 MÉTRICAS FINAIS
    # ======================================================

    logger.info("Etapa 8 - Aplicando metric layer")

    metric_cube_final = apply_metric_layer(

        metric_cube_base

    )

    METRIC_FINAL_PATH.mkdir(parents=True, exist_ok=True)

    metric_final_path = METRIC_FINAL_PATH / "metric_cube_final.parquet"

    metric_cube_final.to_parquet(

        metric_final_path,
        index=False

    )

    logger.info(f"Metric cube final salvo: {metric_final_path}")

    logger.info("========================================")
    logger.info("CONTACT RATE PIPELINE END")
    logger.info("========================================")

    return {
        "client_contact_rate": client_enriched,
        "metric_cube_base": metric_cube_base,
        "metric_cube_final": metric_cube_final
    }
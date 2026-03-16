
# ============================================
# IMPORTS
# ============================================

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
# EXTRACT ORIGINAIS
# ===============================

#from src.load.load_zendesk_raw import load_zendesk_raw
#from src.load.load_mensageria_raw import load_mensageria_raw
#from src.load.load_ultimate_raw import load_ultimate_raw
#from src.load.load_vendas_raw import load_vendas_raw

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

from src.intermediate.ultimate_enrichment import derive_retido_bot


from src.intermediate.global_enrichments import apply_global_enrichments


# ===============================
# MART LAYER
# ===============================

from src.mart.client_contact_rate import build_client_layer
from src.mart.operation_contact_rate import build_operation_layer


# ===============================
# METRICS
# ===============================

from src.metrics.common.generate_base_metrics import generate_base_metrics
from src.metrics.common.apply_metric_layer import apply_metric_layer

# ===============================
# VALIDATION
# ===============================

from src.validation.anomaly_checks import detect_volume_anomaly


# ===============================

logger = setup_logger()


def build_contact_rate_metrics():

    logger.info("========================================")
    logger.info("CONTACT RATE PIPELINE START")
    logger.info("========================================")


    # ======================================================
    # EXTRACT RAW DATASETS
    # ======================================================

    # TBE

    # ======================================================
    # LOAD RAW DATASETS
    # ======================================================

    logger.info("Etapa 2 - Carregando datasets RAW")

    zendesk_raw = load_raw_dataset(RAW_ZENDESK_PATH)
    mensageria_raw = load_raw_dataset(RAW_MENSAGERIA_PATH)
    ultimate_raw = load_raw_dataset(RAW_ULTIMATE_PATH)
    vendas_raw = load_raw_dataset(RAW_VENDAS_PATH)

    logger.info(f"Zendesk RAW: {len(zendesk_raw)} linhas")
    logger.info(f"Mensageria RAW: {len(mensageria_raw)} linhas")
    logger.info(f"Ultimate RAW: {len(ultimate_raw)} linhas")
    logger.info(f"Vendas RAW: {len(vendas_raw)} linhas")

    # ======================================================
    # STAGING
    # ======================================================

    logger.info("Etapa 3 - Staging")

    zendesk_stg = stg_zendesk(zendesk_raw)
    mensageria_stg = stg_mensageria(mensageria_raw)
    ultimate_stg = stg_ultimate(ultimate_raw)
    vendas_stg = stg_vendas(vendas_raw)
        

    logger.info(f"Zendesk STG: {len(zendesk_stg)} linhas")
    logger.info(f"Mensageria STG: {len(mensageria_stg)} linhas")
    logger.info(f"Ultimate STG: {len(ultimate_stg)} linhas")
    logger.info(f"Vendas STG: {len(vendas_stg)} linhas")

    # Lê novamente os vendas_raw .parquets
    vendas_cube_stg = stg_vendas_cube()    
    logger.info(f"Vendas Cube STG: {len(vendas_cube_stg)} linhas")

    # ============================================
    # ZENDESK SEGMENTATION
    # ============================================

    logger.info("Etapa 4 - Segmentação Canais de Atendimento Zendesk")

    zendesk_marketplace, zendesk_whatsapp, zendesk_others = zendesk_segmentation(
        zendesk_stg
    )

    logger.info(f"Marketplace: {len(zendesk_marketplace)}")
    logger.info(f"Whatsapp: {len(zendesk_whatsapp)}")
    logger.info(f"Others: {len(zendesk_others)}")

    # ======================================================
    # CONTACT VALIDATIONS
    # ======================================================

    logger.info("Etapa 5 - Aplicando regras de contatos válidos por canal de atendimento zendesk")    

    marketplace_valid = valid_contacts_marketplace(zendesk_marketplace)
    whatsapp_valid = valid_contacts_whatsapp(zendesk_whatsapp)
    others_valid = valid_contacts_others(zendesk_others)

    logger.info(f"Marketplace válidos: {len(marketplace_valid)}")
    logger.info(f"Whatsapp válidos: {len(whatsapp_valid)}")
    logger.info(f"Others válidos: {len(others_valid)}")

    # ============================================
    # CHANNEL ENRICHMENTS
    # ============================================

    logger.info("Etapa 6 - Enriquecimentos de canais de atendimento Zendesk")

    marketplace_enriched = enrich_marketplace(marketplace_valid)
    whatsapp_enriched = enrich_whatsapp(whatsapp_valid)
    others_enriched = enrich_others(others_valid)


    # ======================================================
    # CLIENT LAYER
    # ======================================================

    logger.info("Etapa 7 - Construindo client_contact_rate")

    client_contacts = build_client_layer(
        marketplace_enriched,
        others_enriched,
        mensageria_stg,
        ultimate_stg
    )

    logger.info(f"Client layer: {len(client_contacts)} linhas")

    # ======================================================
    # CLIENTE LAYER GLOBAL ENRICHMENT
    # ======================================================

    logger.info("Etapa 8 - Enriquecimentos globais")

    client_enriched = apply_global_enrichments(
        client_contacts,
        vendas_stg
    )

    logger.info(f"Client enriched: {len(client_enriched)} linhas")

    # =====================================================
    # ANOMALY CHECKS - CLIENT_ENRICHED
    # =====================================================

    logger.info("Etapa 9 - Análise de Anomalias - client_contact_rate")


    print("\n===================================")
    print("ANOMALIA - id_contato (DIÁRIO)")
    print("===================================")

    detect_volume_anomaly(
        client_enriched,
        date_col="data_contato",
        id_col="id_contato",
        freq="D"
    )


    print("\n===================================")
    print("ANOMALIA - id_contato (SEMANAL)")
    print("===================================")

    detect_volume_anomaly(
        client_enriched,
        date_col="data_contato",
        id_col="id_contato",
        freq="W"
    )


    print("\n===================================")
    print("ANOMALIA - id_pedido (DIÁRIO)")
    print("===================================")

    detect_volume_anomaly(
        client_enriched.dropna(subset=["id_pedido"]),
        date_col="data_contato",
        id_col="id_pedido",
        freq="D"
    )


    print("\n===================================")
    print("ANOMALIA - id_pedido (SEMANAL)")
    print("===================================")

    detect_volume_anomaly(
        client_enriched.dropna(subset=["id_pedido"]),
        date_col="data_contato",
        id_col="id_pedido",
        freq="W"
    )


    # ======================================================
    # SALVAR CLIENT CONTACT RATE
    # ======================================================

    logger.info("Etapa 10 - Salvando client_contact_rate")

    CLIENT_CONTACT_RATE_PATH.mkdir(parents=True, exist_ok=True)

    client_path = CLIENT_CONTACT_RATE_PATH / "client_contact_rate.parquet"

    client_enriched.to_parquet(

        client_path,
        index=False

    )

    client_contact_rate_sample = client_enriched[client_enriched["data_contato"].dt.month.isin([2, 3])].copy()
    client_contact_rate_sample.to_excel(SAMPLE_PATH / "client_contact_rate_sample.xlsx", index=False)

    # ======================================================
    # BUILDING OPERATION  LAYER
    # ======================================================


    logger.info("Etapa 11 - Construindo operation_contact_rate")

    operation_contacts = build_operation_layer(
        marketplace_enriched,
        others_enriched,
        whatsapp_enriched
    )

    logger.info(f"Operation layer: {len(operation_contacts)} linhas")


    # ======================================================
    # OPERATION LAYER GLOBAL ENRICHMENT
    # ======================================================

    logger.info("Etapa 12 - Enriquecimentos globais camada de operação")

    operation_enriched = apply_global_enrichments(
        operation_contacts,
        vendas_stg
    )

    logger.info(f"Operation enriched: {len(operation_enriched)} linhas")

    # =====================================================
    # ANOMALY CHECKS - OPERATION_ENRICHED
    # =====================================================

    logger.info("Etapa 13 - Análise de Anomalias - operation_enriched")


    print("\n===================================")
    print("ANOMALIA - id_contato (DIÁRIO)")
    print("===================================")

    detect_volume_anomaly(
        operation_enriched,
        date_col="data_contato",
        id_col="id_contato",
        freq="D"
    )


    print("\n===================================")
    print("ANOMALIA - id_contato (SEMANAL)")
    print("===================================")

    detect_volume_anomaly(
        operation_enriched,
        date_col="data_contato",
        id_col="id_contato",
        freq="W"
    )


    print("\n===================================")
    print("ANOMALIA - id_pedido (DIÁRIO)")
    print("===================================")

    detect_volume_anomaly(
        operation_enriched.dropna(subset=["id_pedido"]),
        date_col="data_contato",
        id_col="id_pedido",
        freq="D"
    )


    print("\n===================================")
    print("ANOMALIA - id_pedido (SEMANAL)")
    print("===================================")

    detect_volume_anomaly(
        operation_enriched.dropna(subset=["id_pedido"]),
        date_col="data_contato",
        id_col="id_pedido",
        freq="W"
    )


    # ======================================================
    # SALVAR OPERATION CONTACT RATE
    # ======================================================

    logger.info("Etapa 14 - Salvando operation_contact_rate")

    OPERATION_CONTACT_RATE_PATH.mkdir(parents=True, exist_ok=True)

    operation_path = OPERATION_CONTACT_RATE_PATH / "operation_contact_rate.parquet"

    operation_enriched.to_parquet(

        operation_path,
        index=False

    )

    logger.info(f"Arquivo salvo: {operation_path}")

    operation_contact_rate_sample = operation_enriched[operation_enriched["data_contato"].dt.month.isin([2, 3])].copy()
    operation_contact_rate_sample.to_excel(SAMPLE_PATH / "operation_contact_rate_sample.xlsx", index=False)

    # # ======================================================
    # # GERAR MÉTRICAS BASE
    # # ======================================================

    # logger.info("Etapa 7 - Gerando métricas base")

    # metric_cube_base = generate_base_metrics(

    #     client_enriched

    # )

    # METRIC_BASE_PATH.mkdir(parents=True, exist_ok=True)

    # metric_base_path = METRIC_BASE_PATH / "metric_cube_base.parquet"

    # metric_cube_base.to_parquet(

    #     metric_base_path,
    #     index=False

    # )

    # logger.info(f"Metric cube base salvo: {metric_base_path}")

    # # ======================================================
    # # 9 MÉTRICAS FINAIS
    # # ======================================================

    # logger.info("Etapa 8 - Aplicando metric layer")

    # metric_cube_final = apply_metric_layer(

    #     metric_cube_base

    # )

    # METRIC_FINAL_PATH.mkdir(parents=True, exist_ok=True)

    # metric_final_path = METRIC_FINAL_PATH / "metric_cube_final.parquet"

    # metric_cube_final.to_parquet(

    #     metric_final_path,
    #     index=False

    # )

    # logger.info(f"Metric cube final salvo: {metric_final_path}")



    # logger.info("========================================")
    # logger.info("CONTACT RATE PIPELINE END")
    # logger.info("========================================")

    return {
        "client_contact_rate": client_enriched,
        "operation_contact_rate": operation_enriched,
        #"metric_cube_base": metric_cube_base,
        #"metric_cube_final": metric_cube_final
    }
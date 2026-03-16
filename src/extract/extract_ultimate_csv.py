import pandas as pd

from datetime import datetime, timedelta

from src.config.settings import (
    RAW_ULTIMATE_EXPORT_PATH,
    RAW_ULTIMATE_PATH,
    DELTA_DAYS_RAW_EXTRACTION
)

from src.utils.logger import setup_logger

logger = setup_logger()


def extract_ultimate_from_csv(bootstrap=False):

    logger.info("===== EXTRAÇÃO ULTIMATE CSV START =====")

    if not RAW_ULTIMATE_EXPORT_PATH.exists():

        logger.error(
            f"Arquivo não encontrado: {RAW_ULTIMATE_EXPORT_PATH}"
        )

        raise FileNotFoundError(RAW_ULTIMATE_EXPORT_PATH)

    # =================================
    # Leitura CSV
    # =================================

    df = pd.read_csv(
        RAW_ULTIMATE_EXPORT_PATH,
        low_memory=False
    )

    logger.info(f"Linhas carregadas: {len(df)}")

    # =================================
    # Conversão de data
    # =================================

    df["conversation_start_time_br"] = pd.to_datetime(
        df["conversation_start_time_br"],
        errors="coerce"
    )

    logger.info(
        f"Min data: {df['conversation_start_time_br'].min()}"
    )

    logger.info(
        f"Max data: {df['conversation_start_time_br'].max()}"
    )

    # =================================
    # Criar semana ISO
    # =================================

    iso_calendar = df["conversation_start_time_br"].dt.isocalendar()

    df["year"] = iso_calendar.year
    df["week"] = iso_calendar.week

    df["week_label"] = (
        df["year"].astype(str)
        + "-W"
        + df["week"].astype(str).str.zfill(2)
    )

    # garantir diretório RAW
    RAW_ULTIMATE_PATH.mkdir(parents=True, exist_ok=True)

    # =================================
    # Bootstrap
    # =================================

    if bootstrap:

        logger.info("Bootstrap ativo — processando todas as semanas")

        weeks_to_update = df["week_label"].unique()

    else:

        today = datetime.today()

        reprocess_start = today - timedelta(
            days=DELTA_DAYS_RAW_EXTRACTION
        )

        logger.info(
            f"Reprocessando dados desde: {reprocess_start.date()}"
        )

        df_reprocess = df[
            df["conversation_start_time_br"] >= reprocess_start
        ]

        weeks_recent = df_reprocess["week_label"].unique()

        # =================================
        # Garantir inclusão da semana atual
        # =================================

        current_year, current_week, _ = today.isocalendar()

        current_week_label = (
            str(current_year) + "-W" + str(current_week).zfill(2)
        )

        weeks_to_update = set(weeks_recent)

        weeks_to_update.add(current_week_label)

        weeks_to_update = list(weeks_to_update)

    logger.info(
        f"Semanas para atualizar: {weeks_to_update}"
    )

    # =================================
    # Salvar parquet por semana
    # =================================

    for week in weeks_to_update:

        df_week = df[df["week_label"] == week]

        if df_week.empty:

            logger.warning(
                f"Semana {week} sem dados no CSV"
            )

            continue

        file_path = RAW_ULTIMATE_PATH / f"{week}.parquet"

        df_week.to_parquet(file_path, index=False)

        logger.info(
            f"Semana {week} salva | {len(df_week)} linhas"
        )

    logger.info("===== EXTRAÇÃO ULTIMATE CSV END =====")
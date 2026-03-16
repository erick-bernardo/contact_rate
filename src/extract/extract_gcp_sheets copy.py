import pandas as pd
from pathlib import Path

from src.config.settings import RAW_ULTIMATE_PATH, ULTIMATE_SHEETS_URL
from src.utils.logger import setup_logger


logger = setup_logger()


def extract_ultimate_from_sheets(ULTIMATE_SHEETS_URL):

    logger.info("===== EXTRAÇÃO ULTIMATE (Google Sheets) START =====")

    df = pd.read_csv(ULTIMATE_SHEETS_URL)

    logger.info(f"Linhas carregadas do Sheets: {len(df)}")

    # converter data
    df["conversation_start_time_br"] = pd.to_datetime(
        df["conversation_start_time_br"],
        errors="coerce"
    )

    # criar semana ISO
    df["year"] = df["conversation_start_time_br"].dt.isocalendar().year
    df["week"] = df["conversation_start_time_br"].dt.isocalendar().week

    df["week_label"] = (
        df["year"].astype(str)
        + "-W"
        + df["week"].astype(str).str.zfill(2)
    )

    weeks = df["week_label"].unique()

    logger.info(f"Semanas encontradas: {len(weeks)}")

    # salvar parquet por semana
    for week in weeks:

        df_week = df[df["week_label"] == week]

        file_path = RAW_ULTIMATE_PATH / f"{week}.parquet"

        df_week.to_parquet(file_path, index=False)

        logger.info(
            f"Semana {week} salva | {len(df_week)} linhas"
        )

    logger.info("===== EXTRAÇÃO ULTIMATE END =====")
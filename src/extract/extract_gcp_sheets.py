import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from src.config.settings import (
    GOOGLE_SHEETS_KEY,
    GOOGLE_SHEETS_GID,
    RAW_ULTIMATE_PATH,
    DELTA_DAYS_RAW_EXTRACTION
)

from src.utils.logger import setup_logger

logger = setup_logger()


def extract_ultimate_from_sheets():

    logger.info("===== EXTRAÇÃO ULTIMATE (Google Sheets) START =====")

    # conexão com API
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "google_credentials.json",
        scope
    )

    client = gspread.authorize(creds)

    sheet = client.open_by_key(GOOGLE_SHEETS_KEY)

    worksheet = sheet.get_worksheet_by_id(GOOGLE_SHEETS_GID)

    data = worksheet.get_all_records()

    df = pd.DataFrame(data)

    logger.info(f"Linhas carregadas do Sheets: {len(df)}")

    # =========================
    # Conversão de data
    # =========================

    df["conversation_start_time_br"] = pd.to_datetime(
        df["conversation_start_time_br"],
        errors="coerce"
    )

    # =========================
    # Criar semana ISO
    # =========================

    df["year"] = df["conversation_start_time_br"].dt.isocalendar().year
    df["week"] = df["conversation_start_time_br"].dt.isocalendar().week

    df["week_label"] = (
        df["year"].astype(str)
        + "-W"
        + df["week"].astype(str).str.zfill(2)
    )

    weeks = df["week_label"].unique()

    logger.info(f"Semanas encontradas: {len(weeks)}")

    # =========================
    # Salvar parquet por semana
    # =========================

    for week in weeks:

        df_week = df[df["week_label"] == week]

        file_path = RAW_ULTIMATE_PATH / f"{week}.parquet"

        df_week.to_parquet(file_path, index=False)

        logger.info(
            f"Semana {week} salva | {len(df_week)} linhas"
        )

    logger.info("===== EXTRAÇÃO ULTIMATE END =====")
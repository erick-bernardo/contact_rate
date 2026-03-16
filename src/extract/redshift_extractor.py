import pandas as pd

from datetime import datetime, timedelta
from pathlib import Path

from src.extract.redshift_connection import get_redshift_connection
from src.extract.query_loader import load_sql

from src.config.settings import (
    DELTA_DAYS_RAW_EXTRACTION,
    DELTA_BOOTSTRAP_DAYS_RAW_EXTRACTION
)

from src.utils.logger import setup_logger

import warnings

warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable"
)


logger = setup_logger()


# =====================================================
# EXECUTOR SEGURO DE QUERY (evita erro de encoding)
# =====================================================

def execute_query_safe(query):

    conn = get_redshift_connection()

    df = pd.read_sql_query(query, conn)

    conn.close()

    return df


# =====================================================
# GERADOR DE SEMANAS
# =====================================================

def generate_week_ranges(start_date, end_date):

    weeks = []

    current = start_date

    while current < end_date:

        week_start = current - timedelta(days=current.weekday())
        week_end = week_start + timedelta(days=7)

        weeks.append((week_start, week_end))

        current = week_end

    return weeks


# =====================================================
# EXTRATOR GENÉRICO REDSHIFT
# =====================================================

def redshift_weekly_extractor(
    query_name: str,
    date_column: str,
    raw_path: Path,
    bootstrap: bool = False
):

    logger.info(f"===== EXTRAÇÃO {query_name} START =====")

    #engine = get_redshift_engine()

    query_template = load_sql(query_name)

    today = datetime.today()

    # =====================================================
    # DEFINIÇÃO DO PERÍODO DE EXTRAÇÃO
    # =====================================================

    if bootstrap:

        start_date = today - timedelta(
            days=DELTA_BOOTSTRAP_DAYS_RAW_EXTRACTION
        )

        logger.info(
            f"Bootstrap ativo | últimos {DELTA_BOOTSTRAP_DAYS_RAW_EXTRACTION} dias"
        )

    else:

        start_date = today - timedelta(
            days=DELTA_DAYS_RAW_EXTRACTION
        )

        logger.info(
            f"Extração incremental | últimos {DELTA_DAYS_RAW_EXTRACTION} dias"
        )

    end_date = today

    weeks = generate_week_ranges(start_date, end_date)

    # =====================================================
    # LOOP NAS SEMANAS
    # =====================================================

    for week_start, week_end in weeks:

        label = week_start.strftime("%Y-W%U")

        file_path = raw_path / f"{label}.parquet"

        if file_path.exists():

            logger.info(f"Semana {label} já existe. Pulando.")

            continue

        logger.info(
            f"Extraindo semana {label} | "
            f"{week_start.date()} → {week_end.date()}"
        )

        query = query_template.format(
            start_date=week_start.date(),
            end_date=week_end.date()
        )

        df = execute_query_safe(query)

        logger.info(f"Linhas extraídas: {len(df)}")

        if len(df) == 0:

            logger.warning(f"Nenhum dado retornado para {label}")

            continue

        # =====================================================
        # VALIDAÇÃO DE SCHEMA
        # =====================================================

        logger.info(
            f"Colunas retornadas: {list(df.columns)}"
        )

        if date_column not in df.columns:

            logger.error(
                f"Coluna de data '{date_column}' não encontrada"
            )

            raise ValueError(
                f"Coluna {date_column} não encontrada na query"
            )

        # =====================================================
        # TRATAMENTO DA COLUNA DE DATA
        # =====================================================

        df[date_column] = pd.to_datetime(
            df[date_column],
            errors="coerce"
        )

        null_dates = df[date_column].isna().sum()

        if null_dates > 0:

            logger.warning(
                f"{null_dates} registros com data nula em {date_column}"
            )

        logger.info(
            f"{date_column} min: {df[date_column].min()} | "
            f"max: {df[date_column].max()}"
        )

        # =====================================================
        # SALVAR PARQUET
        # =====================================================

        df.to_parquet(file_path, index=False)

        logger.info(f"Arquivo salvo: {file_path}")

    logger.info(f"===== EXTRAÇÃO {query_name} END =====")
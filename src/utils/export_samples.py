import pandas as pd
from datetime import datetime, timedelta

from src.config.settings import DELTA_DAYS_METRICS_REPROCESS


def export_sample(df, date_col, path):

    cutoff_date = (
        datetime.today()
        - timedelta(days=DELTA_DAYS_METRICS_REPROCESS)
    )

    sample_df = df[df[date_col] >= cutoff_date]

    sample_df.to_excel(
        path,
        index=False
    )
import pandas as pd


def save_partition_by_week(df, date_col, base_path):

    df = df.copy()

    df["partition_week"] = df[date_col].dt.strftime("%Y-W%U")

    weeks = df["partition_week"].unique()

    for week in weeks:

        df_week = df[df["partition_week"] == week]

        file_path = base_path / f"{week}.parquet"

        df_week.drop(columns=["partition_week"]).to_parquet(
            file_path,
            index=False
        )

import pandas as pd
from pathlib import Path


def load_raw_dataset(path: Path):

    files = list(path.glob("*.parquet"))

    if not files:
        raise ValueError(f"Nenhum parquet encontrado em {path}")

    dfs = []

    for file in files:

        df = pd.read_parquet(file)

        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)

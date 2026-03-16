from pathlib import Path
import pandas as pd


def load_zendesk_raw():

    base_path = Path(__file__).resolve().parents[2]

    raw_path = base_path / "data" / "raw" / "zendesk"

    files = list(raw_path.glob("*.csv"))

    if len(files) == 0:
        raise ValueError("Nenhum arquivo CSV encontrado em data/raw/zendesk")

    df_list = []

    for file in files:
        print(f"Lendo arquivo: {file.name}")

        df = pd.read_csv(
            file,
            sep=",",
            encoding="utf-8",
            low_memory=False
        )

        df_list.append(df)

    df_final = pd.concat(df_list, ignore_index=True)

    print(f"Total de linhas carregadas: {len(df_final)}")

    return df_final
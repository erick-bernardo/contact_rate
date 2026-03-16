from pathlib import Path
import pandas as pd


def load_ultimate_raw() -> pd.DataFrame:
    """
    Loader da base RAW Ultimate.

    Responsabilidades:
    - localizar arquivos na pasta raw
    - ler todos os CSV
    - concatenar datasets
    - retornar dataframe bruto
    """

    base_path = Path(__file__).resolve().parents[2]

    raw_path = base_path / "data" / "raw" / "ultimate"

    files = list(raw_path.glob("*.csv"))

    if not files:
        raise ValueError(
            "Nenhum arquivo encontrado em data/raw/ultimate"
        )

    dfs = []

    for file in files:

        print(f"Lendo arquivo: {file.name}")

        df = pd.read_csv(
            file,
            sep=",",
            encoding="utf-8",
            low_memory=False
        )

        dfs.append(df)

    df_final = pd.concat(dfs, ignore_index=True)

    print(f"\nTotal de registros carregados: {len(df_final)}")

    return df_final
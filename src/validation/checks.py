import pandas as pd


def validate_schema(df: pd.DataFrame, expected_columns: list):

    current_columns = set(df.columns)
    expected_columns = set(expected_columns)

    missing_columns = expected_columns - current_columns
    extra_columns = current_columns - expected_columns

    if missing_columns:
        raise ValueError(f"Colunas faltando: {missing_columns}")

    if extra_columns:
        print(f"⚠ Colunas extras detectadas: {extra_columns}")

    print("✔ Schema validado")


def check_nulls(df: pd.DataFrame, critical_columns: list):

    for col in critical_columns:

        null_rate = df[col].isna().mean()

        if null_rate > 0:
            print(f"⚠ {col} possui {null_rate:.2%} de valores nulos")

        else:
            print(f"✔ {col} sem nulos")


def check_duplicates(df: pd.DataFrame, id_column: str):

    dup_count = df[id_column].duplicated().sum()

    if dup_count > 0:
        print(f"⚠ {dup_count} duplicados encontrados em {id_column}")

    print(f"✔ {id_column} sem duplicados")


def check_volume(df: pd.DataFrame, min_rows: int):

    if len(df) < min_rows:
        print("⚠ Volume abaixo do esperado")

    print(f"✔ Volume OK: {len(df)} linhas")
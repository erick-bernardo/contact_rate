import pandas as pd
from pathlib import Path



def merge_csvs_to_weekly_parquet(
    input_dir: str,
    output_dir: str,
    date_column: str
):
    print("===== INÍCIO DO PROCESSAMENTO =====")
    
    # Configuração de diretórios
    in_path = Path(input_dir)
    out_path = Path(output_dir)
    
    # Cria o diretório de saída caso ele não exista
    out_path.mkdir(parents=True, exist_ok=True)

    # 1. Mapear e ler todos os CSVs da pasta
    csv_files = list(in_path.glob("*.csv"))
    
    if not csv_files:
        print(f"❌ Nenhum arquivo .csv encontrado no diretório: {in_path}")
        return

    print(f"Encontrados {len(csv_files)} arquivo(s) .csv. Iniciando leitura...")
    
    # Lê cada arquivo e guarda em uma lista, depois une todos (concat)
    lista_dfs = []
    for file in csv_files:
        print(f"  -> Lendo: {file.name}")
        df_temp = pd.read_csv(file)
        lista_dfs.append(df_temp)
        
    df = pd.concat(lista_dfs, ignore_index=True)
    print(f"Total de linhas combinadas: {len(df)}")

    # 2. Validação da coluna
    if date_column not in df.columns:
        raise ValueError(f"A coluna '{date_column}' não foi encontrada. Colunas disponíveis: {list(df.columns)}")

    # 3. Tratamento da Data
    df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
    
    null_dates = df[date_column].isna().sum()
    if null_dates > 0:
        print(f"⚠️ Aviso: {null_dates} registros com data nula ou inválida foram removidos.")
        df = df.dropna(subset=[date_column])

    print(f"Data Min: {df[date_column].min()} | Data Max: {df[date_column].max()}")

    # 4. Criação do Label da Semana 
    # Usando %W para garantir que a semana inicie na SEGUNDA-FEIRA
    week_labels = df[date_column].dt.strftime("%Y-W%W")

    # 5. Agrupamento e Escrita
    for label, group_df in df.groupby(week_labels):
        file_path = out_path / f"{label}.parquet"

        if file_path.exists():
            print(f"⏭️  Semana {label} já existe. Pulando.")
            continue

        print(f"💾 Salvando semana {label} | Linhas: {len(group_df)}")
        group_df.to_parquet(file_path, index=False)

    print("===== PROCESSAMENTO CONCLUÍDO =====")
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
    out_path.mkdir(parents=True, exist_ok=True)

    # 1. Mapear e ler todos os CSVs da pasta
    csv_files = list(in_path.glob("*.csv"))
    
    if not csv_files:
        print(f"❌ Nenhum arquivo .csv encontrado no diretório: {in_path}")
        return

    print(f"Encontrados {len(csv_files)} arquivo(s) .csv. Iniciando leitura...")
    
    lista_dfs = []
    for file in csv_files:
        print(f"  -> Lendo: {file.name}")
        # MUDANÇA 1: Força o ID a ser lido como string desde a raiz
        # low_memory=False evita que o Pandas reclame de tipos mistos ao ler pedaços do CSV
        df_temp = pd.read_csv(file, dtype={'id_pedido_filho': str}, low_memory=False)
        lista_dfs.append(df_temp)
        
    df = pd.concat(lista_dfs, ignore_index=True)
    print(f"Total de linhas combinadas: {len(df)}")


    # =========================================================================
    # FAXINA DE IDs (Trata os CSVs antigos e blinda o Parquet)
    # =========================================================================
    colunas_id = ['id_pedido_pai', 'id_pedido_filho'] # Adicione outros IDs aqui se precisar

    for col in colunas_id:
        if col in df.columns:
            # 1. Força a coluna a ser texto
            df[col] = df[col].astype(str)
            
            # 2. Remove o ".0" do final usando regex (ex: '62262880.0' vira '62262880')
            df[col] = df[col].str.replace(r'\.0$', '', regex=True)
            
            # 3. Transforma os falsos 'nan' (que viraram texto) em nulos reais
            df[col] = df[col].replace({'nan': None, 'None': None, '': None})
            
            # 4. Usa o tipo nativo de String do Pandas (Isso obriga o Parquet a salvar como texto!)
            df[col] = df[col].astype("string")


    colunas_object = df.select_dtypes(include=['object']).columns
    df[colunas_object] = df[colunas_object].astype(str)

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
    week_labels = df[date_column].dt.strftime("%G-W%V")

    # 5. Agrupamento e Escrita
    for label, group_df in df.groupby(week_labels):
        file_path = out_path / f"{label}.parquet"

        if file_path.exists():
            print(f"⏭️  Semana {label} já existe. Pulando.")
            continue

        print(f"💾 Salvando semana {label} | Linhas: {len(group_df)}")
        group_df.to_parquet(file_path, index=False)

    print("===== PROCESSAMENTO CONCLUÍDO =====")
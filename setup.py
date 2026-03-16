import os

# Estrutura de arquivos .py e .ipynb
python_files = [
    "contact_rate_zendesk_v01.py",
    "notebooks/poc_contact_rate.ipynb",
    "src/extract/load_zendesk_raw.py",
    "src/extract/load_ultimate_raw.py",
    "src/extract/load_mensageria_raw.py",
    "src/extract/load_vendas_raw.py",
    "src/staging/stg_zendesk.py",
    "src/staging/stg_ultimate.py",
    "src/staging/stg_mensageria.py",
    "src/staging/stg_vendas.py",
    "src/intermediate/zendesk_segmentation.py",
    "src/intermediate/client_layer/client_contacts_union.py",
    "src/intermediate/operation_layer/operation_zendesk_filter.py",
    "src/mart/build_client_contact_rate.py",
    "src/mart/build_operation_contact_rate.py",
]

# Estrutura de pastas de dados (diretórios vazios)
data_dirs = [
    "data/raw/zendesk",
    "data/raw/ultimate",
    "data/raw/mensageria",
    "data/raw/vendas",
    "data/staged/zendesk",
    "data/staged/ultimate",
    "data/staged/mensageria",
    "data/staged/vendas",
    "data/intermediate/zendesk_segments/zendesk_marketplace",
    "data/intermediate/zendesk_segments/zendesk_whatsapp",
    "data/intermediate/zendesk_segments/zendesk_others",
    "data/intermediate/client_layer",
    "data/intermediate/operation_layer/zendesk_marketplace_operation",
    "data/intermediate/operation_layer/zendesk_whatsapp_operation",
    "data/intermediate/operation_layer/zendesk_others_operation",
    "data/mart/client_contact_rate",
    "data/mart/operation_contact_rate",
]

base_dir = "contact_rate_project_template"

def create_structure():
    print(f"📁 Iniciando criação do projeto em: {base_dir}\n")
    
    # 1. Criar arquivos Python e Notebooks
    for file_path in python_files:
        full_path = os.path.join(base_dir, file_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, 'w') as f:
            pass
        print(f"📄 Arquivo criado: {file_path}")

    # 2. Criar diretórios de dados
    for dir_path in data_dirs:
        full_path = os.path.join(base_dir, dir_path)
        os.makedirs(full_path, exist_ok=True)
        # Adiciona um arquivo .gitkeep para garantir que a pasta seja rastreada pelo Git
        with open(os.path.join(full_path, ".gitkeep"), 'w') as f:
            pass
        print(f"📂 Diretório de dados criado: {dir_path}")

    print(f"\n✅ Tudo pronto! Estrutura de dados e código espelhada com sucesso.")

if __name__ == "__main__":
    create_structure()
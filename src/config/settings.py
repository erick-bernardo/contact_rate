from pathlib import Path


# =====================================================
# ROOT DO PROJETO
# =====================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]


# =====================================================
# DATA PATHS
# =====================================================

DATA_PATH = PROJECT_ROOT / "data"

RAW_PATH = DATA_PATH / "raw"
STAGED_PATH = DATA_PATH / "staged"
MART_PATH = DATA_PATH / "mart"
METRICS_PATH = DATA_PATH / "metrics"
LOG_PATH = DATA_PATH / "logs"


# =====================================================
# RAW DATASETS
# =====================================================

RAW_VENDAS_PATH = RAW_PATH / "vendas"
RAW_ZENDESK_PATH = RAW_PATH / "zendesk"
RAW_MENSAGERIA_PATH = RAW_PATH / "mensageria"
RAW_ULTIMATE_PATH = RAW_PATH / "ultimate"


# =====================================================
# MART DATASETS
# =====================================================

CLIENT_CONTACT_RATE_PATH = MART_PATH / "client" / "client_contact_rate.parquet"
OPERATION_CONTACT_RATE_PATH = MART_PATH / "operation" / "operation_contact_rate.parquet"


# =====================================================
# METRIC CUBES
# =====================================================

CLIENT_METRIC_CUBE_BASE = METRICS_PATH / "client" / "metric_cube_base.parquet"
CLIENT_METRIC_CUBE_FINAL = METRICS_PATH / "client" / "metric_cube_final.parquet"

OPERATION_METRIC_CUBE_BASE = METRICS_PATH / "operation" / "metric_cube_base.parquet"
OPERATION_METRIC_CUBE_FINAL = METRICS_PATH / "operation" / "metric_cube_final.parquet"

# =====================================================
# SAMPLE DATA (VALIDATION FILES)
# =====================================================

SAMPLE_PATH = DATA_PATH / "sample"

CLIENT_SAMPLE_FILE = SAMPLE_PATH / "client_contact_rate_sample.xlsx"
OPERATION_SAMPLE_FILE = SAMPLE_PATH / "operation_contact_rate_sample.xlsx"

CLIENT_METRIC_BASE_SAMPLE = SAMPLE_PATH / "client_metric_cube_base_sample.xlsx"
CLIENT_METRIC_FINAL_SAMPLE = SAMPLE_PATH / "client_metric_cube_final_sample.xlsx"

OPERATION_METRIC_BASE_SAMPLE = SAMPLE_PATH / "operation_metric_cube_base_sample.xlsx"
OPERATION_METRIC_FINAL_SAMPLE = SAMPLE_PATH / "operation_metric_cube_final_sample.xlsx"

# =====================================================
# PIPELINE PARAMETERS
# =====================================================

# dias no passado para atualizar RAW
DELTA_DAYS_RAW_EXTRACTION = 30
DELTA_BOOTSTRAP_DAYS_RAW_EXTRACTION = 60

# dias no passado para recalcular métricas
DELTA_DAYS_METRICS_REPROCESS = 30


# =====================================================
# DATA PARTITION CONFIG
# =====================================================

RAW_PARTITION_FORMAT = "%Y-W%U"

# =====================================================
# LOGS
# =====================================================

LOGS_PATH = DATA_PATH / "logs"

# =====================================================
# GOOGLE ULTIMATE EXTRACTION
# =====================================================

RAW_ULTIMATE_EXPORT_PATH = (
    PROJECT_ROOT / "data/raw/gcp_exports/ultimate/ultimate_export.csv"
)

RAW_ULTIMATE_PATH = (
    PROJECT_ROOT / "data/raw/ultimate"
)


#GOOGLE_SHEETS_KEY = "1sinMPoOW6CKNxsTHNtTr35H9P7GYsXOXcyhRHdqJHwU"
#GOOGLE_SHEETS_GID = 1115439955


def ensure_directories():

    paths = [
        RAW_PATH,
        STAGED_PATH,
        MART_PATH,
        METRICS_PATH,
        RAW_VENDAS_PATH,
        RAW_ZENDESK_PATH,
        RAW_MENSAGERIA_PATH,
        RAW_ULTIMATE_PATH,
        LOGS_PATH
    ]

    for path in paths:
        path.mkdir(parents=True, exist_ok=True)
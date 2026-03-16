from pathlib import Path
from src.config.settings import PROJECT_ROOT


SQL_PATH = PROJECT_ROOT / "sql"


def load_sql(query_name: str):

    query_file = SQL_PATH / query_name

    with open(query_file, "r", encoding="utf-8") as f:

        query = f.read()

    return query
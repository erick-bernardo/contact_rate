import logging
from src.config.settings import DATA_PATH
from pathlib import Path

LOG_PATH = DATA_PATH / "logs"
LOG_PATH.mkdir(exist_ok=True)


def setup_logger():

    logger = logging.getLogger("pipeline_logger")

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    # arquivo
    file_handler = logging.FileHandler(
        LOG_PATH / "pipeline.log",
        encoding="utf-8"
    )

    file_handler.setFormatter(formatter)

    # console
    console_handler = logging.StreamHandler()

    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
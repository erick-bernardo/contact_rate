from src.pipeline.build_contact_rate_metrics import build_contact_rate_metrics
from src.config.settings import ensure_directories


def main():

    ensure_directories()
    build_contact_rate_metrics()


if __name__ == "__main__":
    main()
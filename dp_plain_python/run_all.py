import logging

from dp_plain_python.extract.run_extract import extract_into_staging
from dp_plain_python.load.run_load import load_into_storage

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def main():
    log.info(f"Data Pipeline started.")

    extract_into_staging()
    load_into_storage()
    # Transform
    # Analytics


if __name__ == "__main__":
    main()

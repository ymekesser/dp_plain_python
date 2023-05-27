import logging

from dp_plain_python.extract.run_extract import extract_into_staging
from dp_plain_python.load.run_load import load_into_storage
from dp_plain_python.transform.run_analytics_transform import transform_for_analytics
from dp_plain_python.analytics.run_analytics import run_analytics

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def main():
    log.info(f"Data Pipeline started.")

    extract_into_staging()
    load_into_storage()
    transform_for_analytics()
    run_analytics()


if __name__ == "__main__":
    main()

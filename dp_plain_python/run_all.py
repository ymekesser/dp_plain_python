import logging
import sys

from dp_plain_python.extract.run_extract import extract_into_staging
from dp_plain_python.load.run_load import load_into_storage
from dp_plain_python.transform.run_analytics_transform import transform_for_analytics
from dp_plain_python.analytics.run_analytics import run_analytics
from dp_plain_python.environment import config

if config.get_logging_setting("Enabled") != "True":
    logging.disable()

log_format = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(
    level=config.get_logging_setting("Level").upper(),
    stream=sys.stdout,
    format=log_format,
)

log = logging.getLogger(__name__)


def main():
    log.info(f"Data Pipeline started.")

    if config.get_pipeline_setting("ExtractEnabled") == "True":
        extract_into_staging()
    if config.get_pipeline_setting("LoadEnabled") == "True":
        load_into_storage()
    if config.get_pipeline_setting("TransformAnalyticsEnabled") == "True":
        transform_for_analytics()
    if config.get_pipeline_setting("AnalyticsEnabled") == "True":
        run_analytics()

    log.info(f"Data Pipeline completed.")


if __name__ == "__main__":
    main()

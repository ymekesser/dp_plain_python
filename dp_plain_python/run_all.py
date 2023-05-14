import logging

from dp_plain_python.extract.run_extract import (
    extract_resale_flat_prices,
    extract_mrt_stations,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def main():
    log.info(f"Data Pipeline started.")

    # Extract
    df = extract_resale_flat_prices()
    df2 = extract_mrt_stations()

    print(df.head())
    print(df2.head())

    # Load

    # Transform
    # Analytics

import logging
import pandas as pd

log = logging.getLogger(__name__)

resale_flat_prices_path = (
    "..\\data\\resale-flat-prices-based-on-registration-date-from-jan-2017-onwards.csv"
)
mrt_stations_path = "..\\data\\mrt_stations.xlsx"


def extract_resale_flat_prices() -> pd.DataFrame:
    log.info(f"Loading Resale Flat prices from CSV  file: {mrt_stations_path}")

    df = pd.read_csv(resale_flat_prices_path)

    log.info(
        f"Resale Flat prices  data successfully loaded. Rows: {df.shape[0]} Columns: {df.shape[1]}"
    )

    return df


def extract_mrt_stations() -> pd.DataFrame:
    log.info(f"Loading MRT station data from Excel file: {mrt_stations_path}")

    df = pd.read_excel(mrt_stations_path, sheet_name="Sheet1")

    log.info(
        f"MRT station data successfully loaded. Rows: {df.shape[0]} Columns: {df.shape[1]}"
    )

    return df

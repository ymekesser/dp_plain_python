import logging
from os import makedirs
from pathlib import Path
import pandas as pd
from dp_plain_python.transform.clean_malls import get_cleaned_malls_with_geolocation
from dp_plain_python.transform.clean_mrt_stations import (
    get_cleaned_mrt_stations_with_geolocation,
)
from dp_plain_python.transform.clean_resale_prices import get_cleaned_resale_prices

log = logging.getLogger(__name__)

# todo make configurable
storage_path = "local_data\\storage"
resale_flat_prices_filename = "resale_flat_prices.csv"
mrt_stations_filename = "mrt_stations.csv"
mrt_geodata_filename = "mrt_geodata.csv"
mall_geodata_filename = "mall_geodata.csv"
transformed_analytics_path = "local_data\\transformed_analytics"


def transform_for_analytics() -> None:
    log.info("Starting Transformation Step for analytics")

    df_resale_flat_prices = _read_from_storage(resale_flat_prices_filename)
    df_mrt_stations = _read_from_storage(mrt_stations_filename)
    df_mrt_geodata = _read_from_storage(mrt_geodata_filename)
    df_mall_geodata = _read_from_storage(mall_geodata_filename)

    df_mrt_stations = get_cleaned_mrt_stations_with_geolocation(
        df_mrt_stations, df_mrt_geodata
    )
    df_mall_geodata = get_cleaned_malls_with_geolocation(df_mall_geodata)
    df_resale_flat_prices = get_cleaned_resale_prices(df_resale_flat_prices)

    # Todo: Calculate closest mrts/malls per entry in df_resale_flat_prices

    df_feature_set = df_resale_flat_prices

    _store_transformed_output(df_feature_set, "feature_set.csv")
    _store_transformed_output(df_mrt_stations, "mrt_temp.csv")
    _store_transformed_output(df_mall_geodata, "mall_temp.csv")


def _read_from_storage(filename: str) -> pd.DataFrame:
    log.info(f"Loading {filename} from storage for transformation")
    path = Path(storage_path) / filename

    return pd.read_csv(path)


def _store_transformed_output(df: pd.DataFrame, filename: str) -> None:
    dst = Path(transformed_analytics_path)
    makedirs(dst, exist_ok=True)

    df.to_csv(dst / filename)

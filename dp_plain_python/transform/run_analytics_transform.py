import logging
from os import makedirs
from pathlib import Path
import pandas as pd
from dp_plain_python.utils.coalesce_columns import coalesce_colums

from dp_plain_python.utils.select_columns import _select_columns

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

    df_mrt_stations = _get_cleaned_mrt_stations_with_geolocation(
        df_mrt_stations, df_mrt_geodata
    )
    df_mall_geodata = _get_cleaned_malls_with_geolocation(df_mall_geodata)
    df_resale_flat_prices = _get_cleaned_resale_prices(df_resale_flat_prices)

    # Calculate closest mrts/malls per entry in df_resale_flat_prices

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


def _get_cleaned_mrt_stations_with_geolocation(
    df_mrt_stations: pd.DataFrame, df_mrt_geodata: pd.DataFrame
) -> pd.DataFrame:
    df_mrt_stations = _select_columns(
        df_mrt_stations,
        {"Name": "name", "Code": "code", "Opening": "opening"},
    )

    df_mrt_stations = _remove_duplicate_stations(df_mrt_stations)
    df_mrt_stations = _remove_planned_stations(df_mrt_stations)
    df_mrt_stations = _get_number_of_lines(df_mrt_stations)

    df_mrt_geodata = _select_columns(
        df_mrt_geodata,
        {
            "tags.name": "name",
            "lat": "latitude",
            "lon": "longitude",
        },
    )

    # Some stations appear as multiple nodes on the map,
    # They are clustered together, so we take the mean
    df_mrt_geodata = df_mrt_geodata.groupby("name").mean()

    # Join with geodata
    return pd.merge(df_mrt_stations, df_mrt_geodata, how="inner", on="name")


def _remove_duplicate_stations(df_mrt_stations):
    # For interchanges the station is listed once per line.
    # we only care about the individual stations
    df_mrt_stations = df_mrt_stations.drop_duplicates(subset=["name"])

    return df_mrt_stations


def _get_number_of_lines(df_mrt_stations: pd.DataFrame) -> pd.DataFrame:
    # For interchanges with other lines, a station has multiple codes, separated by space.
    # E.g. Jurong East servicing both North-South and East-West lines has the code "NS1 EW24"
    df_mrt_stations["No of Lines"] = (
        df_mrt_stations["code"].str.replace("", "").str.split().apply(len)
    )

    return df_mrt_stations


def _remove_planned_stations(df_mrt_stations: pd.DataFrame) -> pd.DataFrame:
    # Format Opening as datetime
    # Remove all rows in df_mrt_stations where the "Opening" column is not a date.
    # Some planned stations have e.g. mid-2034 as an opening date
    df_mrt_stations["opening"] = pd.to_datetime(
        df_mrt_stations["opening"], errors="coerce"
    )
    df_mrt_stations = df_mrt_stations.dropna(subset=["opening"])

    # We ignore stations which open in the future
    cutoff_date = pd.Timestamp("2023-05-01")
    mask = df_mrt_stations["opening"] > cutoff_date
    df_mrt_stations = df_mrt_stations.drop(df_mrt_stations[mask].index)

    return df_mrt_stations


def _get_cleaned_malls_with_geolocation(df_mall_geodata: pd.DataFrame) -> pd.DataFrame:
    # Depending on the type of element, the lat/long are in different columns
    df_mall_geodata = coalesce_colums(
        df_mall_geodata, ["lat", "center.lat"], "latitude"
    )
    df_mall_geodata = coalesce_colums(
        df_mall_geodata, ["lon", "center.lon"], "longitude"
    )

    df_mall_geodata = _select_columns(
        df_mall_geodata,
        {
            "tags.name": "name",
            "latitude": "latitude",
            "longitude": "longitude",
        },
    )

    df_mall_geodata = _remove_nameless_malls(df_mall_geodata)
    df_mall_geodata = _remove_duplicate_malls(df_mall_geodata)

    return df_mall_geodata


def _remove_nameless_malls(df_mall_geodata: pd.DataFrame) -> pd.DataFrame:
    df_mall_geodata = df_mall_geodata.dropna(subset=["name"])

    return df_mall_geodata


def _remove_duplicate_malls(df_mall_geodata: pd.DataFrame) -> pd.DataFrame:
    # Some malls, e.g. Mustafa Centre, appear multiple times.
    # The locations are clustered together, so we take the mean of lat/long
    df_mall_geodata = df_mall_geodata.groupby("name").mean()

    return df_mall_geodata


def _get_cleaned_resale_prices(df_resale_flat_prices: pd.DataFrame) -> pd.DataFrame:
    return _select_columns(
        df_resale_flat_prices,
        {
            "town": "town",
            "flat_type": "flat_type",
            "street_name": "street_name",
            "storey_range": "storey_range",
            "floor_area_sqm": "floor_area_sqm",
            "flat_model": "flat_model",
            "lease_commence_date": "lease_commence_date",
            "remaining_lease": "remaining_lease",
            "resale_price": "resale_price",
        },
    )

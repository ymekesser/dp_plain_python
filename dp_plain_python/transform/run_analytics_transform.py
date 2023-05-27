import logging
import pandas as pd
from dp_plain_python.transform.clean_address import (
    get_cleaned_addresses_with_geolocation,
)
from dp_plain_python.transform.clean_malls import get_cleaned_malls_with_geolocation
from dp_plain_python.transform.clean_mrt_stations import (
    get_cleaned_mrt_stations_with_geolocation,
)
from dp_plain_python.transform.clean_resale_prices import get_cleaned_resale_prices

from scipy.spatial import cKDTree
from math import radians

from dp_plain_python.environment import config, file_storage


log = logging.getLogger(__name__)

storage_path = config.get_location("Storage")
transformed_analytics_path = config.get_location("TransformedAnalytics")

resale_flat_prices_filename = config.get_storage_filename("ResaleFlatPrices")
mrt_stations_filename = config.get_storage_filename("MrtStations")
mrt_geodata_filename = config.get_storage_filename("MrtGeodata")
mall_geodata_filename = config.get_storage_filename("MallGeodata")
address_geodata_filename = config.get_storage_filename("HdbAddressGeodata")

storage = file_storage.LocalFileStorage()


def transform_for_analytics() -> None:
    log.info("Starting Transformation Step for analytics")

    storage.ensure_directory(transformed_analytics_path)

    df_resale_flat_prices = storage.read_dataframe(
        storage_path / resale_flat_prices_filename
    )
    df_mrt_stations = storage.read_dataframe(storage_path / mrt_stations_filename)
    df_mrt_geodata = storage.read_dataframe(storage_path / mrt_geodata_filename)
    df_mall_geodata = storage.read_dataframe(storage_path / mall_geodata_filename)
    df_address_geodata = storage.read_dataframe(storage_path / address_geodata_filename)

    df_resale_flat_prices = get_cleaned_resale_prices(df_resale_flat_prices)
    df_mrt_stations = get_cleaned_mrt_stations_with_geolocation(
        df_mrt_stations, df_mrt_geodata
    )
    df_mall_geodata = get_cleaned_malls_with_geolocation(df_mall_geodata)
    df_address_geodata = get_cleaned_addresses_with_geolocation(df_address_geodata)

    df_feature_set = pd.merge(
        df_resale_flat_prices,
        df_address_geodata,
        how="left",
        on=["block", "street_name"],
    )

    missing_latitude_count = df_feature_set["latitude"].isnull().sum()
    log.info(
        f"Number of rows with missing address location: {missing_latitude_count} out of {df_feature_set.shape[0]}"
    )
    df_feature_set = df_feature_set.dropna(subset=["latitude"])

    df_feature_set = _add_closest_mrt(df_feature_set, df_mrt_stations)
    df_feature_set = _add_closest_mall(df_feature_set, df_mall_geodata)
    df_feature_set = _add_distance_to_cbd(df_feature_set)

    _store_transformed_output(df_feature_set, "feature_set.csv")


def _add_closest_mrt(df_feature_set, df_mrt_stations):
    df_feature_set = _find_closest_location(
        df_feature_set, df_mrt_stations, "closest_mrt"
    )
    return df_feature_set


def _add_closest_mall(df_feature_set, df_mall_geodata):
    df_feature_set = _find_closest_location(
        df_feature_set, df_mall_geodata, "closest_mall"
    )

    return df_feature_set


def _add_distance_to_cbd(df_feature_set):
    cbd_location = pd.DataFrame.from_dict(
        {
            "latitude": [1.280602347559877],
            "longitude": [103.85040609311484],
            "name": "CBD",
        }
    )

    df_feature_set = _find_closest_location(df_feature_set, cbd_location, "cbd")

    return df_feature_set


def _find_closest_location(
    df_feature_set: pd.DataFrame, df_locations: pd.DataFrame, location_type: str
) -> pd.DataFrame:
    EARTH_RADIUS = 6371

    # Convert latitude and longitude to radians
    df_feature_set["latitude_rad"] = df_feature_set["latitude"].apply(radians)
    df_feature_set["longitude_rad"] = df_feature_set["longitude"].apply(radians)
    df_locations["latitude_rad"] = df_locations["latitude"].apply(radians)
    df_locations["longitude_rad"] = df_locations["longitude"].apply(radians)

    # Create a KDTree for the location dataframe
    location_tree = cKDTree(df_locations[["latitude_rad", "longitude_rad"]])

    # Query the KDTree for each point in the points dataframe
    distances, indices = location_tree.query(
        df_feature_set[["latitude_rad", "longitude_rad"]], k=1
    )

    # Get the closest entry from the location dataframe
    df_feature_set[f"{location_type}"] = df_locations.loc[indices, "name"].values  # type: ignore
    df_feature_set[f"distance_to_{location_type}"] = distances * EARTH_RADIUS * 1000

    # Drop the intermediate columns
    df_feature_set.drop(["latitude_rad", "longitude_rad"], axis=1, inplace=True)

    return df_feature_set


def _store_transformed_output(df: pd.DataFrame, filename: str) -> None:
    log.info(f"Storing {filename} to transformed (analytics)")

    storage.write_dataframe(df, transformed_analytics_path / filename)

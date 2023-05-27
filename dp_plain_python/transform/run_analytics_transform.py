import logging
from os import makedirs
from pathlib import Path
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

from dp_plain_python.utils.select_columns import select_columns


log = logging.getLogger(__name__)

# todo make configurable
storage_path = "local_data\\storage"
resale_flat_prices_filename = "resale_flat_prices.csv"
mrt_stations_filename = "mrt_stations.csv"
mrt_geodata_filename = "mrt_geodata.csv"
mall_geodata_filename = "mall_geodata.csv"
address_geodata_filename = "address_geodata.csv"
transformed_analytics_path = "local_data\\transformed_analytics"


def transform_for_analytics() -> None:
    log.info("Starting Transformation Step for analytics")

    df_resale_flat_prices = _read_from_storage(resale_flat_prices_filename)
    df_mrt_stations = _read_from_storage(mrt_stations_filename)
    df_mrt_geodata = _read_from_storage(mrt_geodata_filename)
    df_mall_geodata = _read_from_storage(mall_geodata_filename)
    df_address_geodata = _read_from_storage(address_geodata_filename)

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
        f"Number of rows with missing latitude: {missing_latitude_count} out of {df_feature_set.shape[0]}"
    )
    df_feature_set = df_feature_set.dropna(subset=["latitude"])

    df_feature_set = _find_closest_location(
        df_feature_set, df_mrt_stations, "closest_mrt"
    )
    df_feature_set = _find_closest_location(
        df_feature_set, df_mall_geodata, "closest_mall"
    )

    cbd_location = pd.DataFrame.from_dict(
        {
            "latitude": [1.280602347559877],
            "longitude": [103.85040609311484],
            "name": "CBD",
        }
    )
    print(cbd_location)
    df_feature_set = _find_closest_location(df_feature_set, cbd_location, "cbd")

    _store_transformed_output(df_feature_set, "feature_set.csv")


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


def _read_from_storage(filename: str) -> pd.DataFrame:
    log.info(f"Loading {filename} from storage for transformation")
    path = Path(storage_path) / filename

    return pd.read_csv(path)


def _store_transformed_output(df: pd.DataFrame, filename: str) -> None:
    log.info(f"Storing {filename} to transformed (analytics)")
    dst = Path(transformed_analytics_path)
    makedirs(dst, exist_ok=True)

    df.to_csv(dst / filename)

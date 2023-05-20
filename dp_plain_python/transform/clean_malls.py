import pandas as pd
from dp_plain_python.utils.coalesce_columns import coalesce_colums
from dp_plain_python.utils.select_columns import select_columns


def get_cleaned_malls_with_geolocation(df_mall_geodata: pd.DataFrame) -> pd.DataFrame:
    # Depending on the type of element, the lat/long are in different columns
    df_mall_geodata = coalesce_colums(
        df_mall_geodata, ["lat", "center.lat"], "latitude"
    )
    df_mall_geodata = coalesce_colums(
        df_mall_geodata, ["lon", "center.lon"], "longitude"
    )

    df_mall_geodata = select_columns(
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

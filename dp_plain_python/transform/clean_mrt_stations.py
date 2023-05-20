import pandas as pd
from dp_plain_python.utils.select_columns import select_columns


def get_cleaned_mrt_stations_with_geolocation(
    df_mrt_stations: pd.DataFrame, df_mrt_geodata: pd.DataFrame
) -> pd.DataFrame:
    df_mrt_stations = select_columns(
        df_mrt_stations,
        {"Name": "name", "Code": "code", "Opening": "opening"},
    )

    df_mrt_stations = _remove_duplicate_stations(df_mrt_stations)
    df_mrt_stations = _remove_planned_stations(df_mrt_stations)
    df_mrt_stations = _get_number_of_lines(df_mrt_stations)

    df_mrt_geodata = select_columns(
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

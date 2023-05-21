import pandas as pd
from dp_plain_python.utils.select_columns import select_columns


def get_cleaned_addresses_with_geolocation(
    df_address_geodata: pd.DataFrame,
) -> pd.DataFrame:
    df_address_geodata = _remove_untrusted_entries(df_address_geodata)
    df_address_geodata = _remove_duplicates(df_address_geodata)

    df_address_geodata = select_columns(
        df_address_geodata,
        {
            "block": "block",
            "street_name": "street_name",
            "latitude": "latitude",
            "longitude": "longitude",
            "postal_code": "postal_code",
            "confidence": "confidence",
            "type": "type",
        },
    )

    return df_address_geodata


def _remove_untrusted_entries(
    df_address_geodata: pd.DataFrame,
) -> pd.DataFrame:
    # The geolocation service used didn't work perfectly, some results are wrong
    # Remove those with low confidence or wrong type

    low_confidence = df_address_geodata["confidence"] < 1
    wrong_type = df_address_geodata["type"] != "address"
    df_address_geodata = df_address_geodata.drop(
        df_address_geodata[low_confidence & wrong_type].index
    )

    return df_address_geodata


def _remove_duplicates(df_mrt_stations):
    # There might be some duplicates
    df_mrt_stations = df_mrt_stations.drop_duplicates(subset=["block", "street_name"])

    return df_mrt_stations

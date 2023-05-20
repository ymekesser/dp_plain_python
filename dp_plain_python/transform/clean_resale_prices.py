import pandas as pd
from dp_plain_python.utils.select_columns import select_columns


def get_cleaned_resale_prices(df_resale_flat_prices: pd.DataFrame) -> pd.DataFrame:
    df_resale_flat_prices = _get_remaining_lease_in_months(df_resale_flat_prices)
    df_resale_flat_prices = _get_storey_median(df_resale_flat_prices)

    return select_columns(
        df_resale_flat_prices,
        {
            "town": "town",
            "flat_type": "flat_type",
            "street_name": "street_name",
            "storey_median": "storey_median",
            "floor_area_sqm": "floor_area_sqm",
            "flat_model": "flat_model",
            "lease_commence_date": "lease_commence_date",
            "remaining_lease_in_months": "remaining_lease_in_months",
            "resale_price": "resale_price",
        },
    )


def _get_remaining_lease_in_months(df_resale_flat_prices: pd.DataFrame) -> pd.DataFrame:
    df_resale_flat_prices["remaining_lease_in_months"] = df_resale_flat_prices[
        "remaining_lease"
    ].apply(_parse_duration)

    return df_resale_flat_prices


def _parse_duration(duration_string: str) -> int:
    # Remaining lease always follows the same pattern,
    # either e.g. 56 years 09 months
    # or e.g. 63 years
    parts = duration_string.split()

    years = int(parts[0])
    months = int(parts[2]) if len(parts) > 2 else 0

    return years * 12 + months


def _get_storey_median(df_resale_flat_prices: pd.DataFrame) -> pd.DataFrame:
    # Floor level is given as a range, e.g. "10 TO 12"
    # To simplify, we take the Median

    df_resale_flat_prices["storey_median"] = df_resale_flat_prices[
        "storey_range"
    ].apply(_calculate_median)

    return df_resale_flat_prices


def _calculate_median(storey_range: str) -> int:
    [lower, upper] = map(int, storey_range.split(" TO "))

    return (lower + upper) // 2

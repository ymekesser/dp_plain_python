import pandas as pd
from datetime import datetime
from dp_plain_python.utils.select_columns import select_columns


def get_cleaned_resale_prices(df_resale_flat_prices: pd.DataFrame) -> pd.DataFrame:
    df_resale_flat_prices = _get_remaining_lease_in_months(df_resale_flat_prices)
    df_resale_flat_prices = _get_storey_median(df_resale_flat_prices)
    df_resale_flat_prices = _get_rooms(df_resale_flat_prices)

    return select_columns(
        df_resale_flat_prices,
        {
            "town": "town",
            "block": "block",
            "street_name": "street_name",
            "storey_median": "storey_median",
            "floor_area_sqm": "floor_area_sqm",
            "room_no": "room_no",
            "flat_model": "flat_model",
            "lease_commence_date": "lease_commence_date",
            "remaining_lease_in_months": "remaining_lease_in_months",
            "resale_price": "resale_price",
        },
    )


def _get_remaining_lease_in_months(df_resale_flat_prices: pd.DataFrame) -> pd.DataFrame:
    def apply_functions(row):
        val = row["remaining_lease"]
        if pd.isna(val):
            return _calculate_remaining_lease(row["lease_commence_date"], row["month"])
        elif isinstance(val, str):
            return _parse_duration_string(val)
        elif isinstance(val, int):
            return _parse_duration_years(val)

    df_resale_flat_prices["remaining_lease_in_months"] = df_resale_flat_prices.apply(
        apply_functions, axis=1
    )

    return df_resale_flat_prices


def _parse_duration_string(duration_string: str) -> int:
    # Remaining lease always follows the same pattern,
    # either e.g. 56 years 09 months
    # or e.g. 63 years
    parts = duration_string.split()

    years = int(parts[0])
    months = int(parts[2]) if len(parts) > 2 else 0

    return years * 12 + months


def _parse_duration_years(years: int) -> int:
    # If remaining lease is given as years only,
    # convert it into months

    return years * 12


def _calculate_remaining_lease(lease_commence_date: int, resale_date_str: str) -> int:
    # If we don't have the remaining lease, we can calculate it
    # from the lease commence date and the date of resale
    # Months are ignored as the lease commence date only specifies the year.

    resale_date = datetime.strptime(resale_date_str, "%d/%m/%Y")
    resale_year = resale_date.year

    return (resale_year - lease_commence_date) * 12


def _get_storey_median(df_resale_flat_prices: pd.DataFrame) -> pd.DataFrame:
    df_resale_flat_prices["storey_median"] = df_resale_flat_prices[
        "storey_range"
    ].apply(_calculate_median)

    return df_resale_flat_prices


def _calculate_median(storey_range: str) -> int:
    # Floor level is given as a range, e.g. "10 TO 12"
    # To simplify, we take the Median

    [lower, upper] = map(int, storey_range.split(" TO "))

    return (lower + upper) // 2


def _get_rooms(df_resale_flat_prices: pd.DataFrame) -> pd.DataFrame:
    df_resale_flat_prices["room_no"] = df_resale_flat_prices["flat_type"].apply(
        _parse_rooms
    )

    return df_resale_flat_prices


def _parse_rooms(flat_type: str) -> int:
    # Flat time indicates number of rooms, e.g. "3 Room"
    # "Executive" flats have 5 rooms plus a study which we treat as an additional room
    # "Multi-Generation" flats have 6 rooms as well

    if flat_type == "EXECUTIVE" or flat_type == "MULTI-GENERATION":
        return 6

    split = flat_type.split()
    rooms_no = int(split[0])

    return rooms_no

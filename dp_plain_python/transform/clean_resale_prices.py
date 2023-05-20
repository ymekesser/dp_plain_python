import pandas as pd
from utils.select_columns import select_columns


def get_cleaned_resale_prices(df_resale_flat_prices: pd.DataFrame) -> pd.DataFrame:
    return select_columns(
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

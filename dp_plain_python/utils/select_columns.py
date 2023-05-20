import pandas as pd


def select_columns(df: pd.DataFrame, selector: dict[str, str]) -> pd.DataFrame:
    return df.rename(columns=selector)[[*selector.values()]]

import json
from pathlib import Path
import pandas as pd
from os import makedirs
import logging

log = logging.getLogger(__name__)

# todo make configurable
staging_path = "local_data\\staging"
storage_path = "local_data\\storage"
resale_flat_prices_filename = (
    "resale-flat-prices-based-on-registration-date-from-jan-2017-onwards.csv"
)
mrt_stations_filename = "mrt_stations.xlsx"
mrt_geodata_filename = "mrt_geodata.json"
mall_geodata_filename = "mall_geodata.json"


def load_into_storage():
    log.info(f"Starting Loading Step.")

    makedirs(storage_path, exist_ok=True)

    _load_resale_flat_prices()
    _load_mrt_stations()
    _load_mrt_geodata()
    _load_mall_geodata()


def _load_resale_flat_prices():
    log.info(f"Loading {resale_flat_prices_filename} from staging into storage")

    source = Path(staging_path) / resale_flat_prices_filename

    df = pd.read_csv(source)

    _store_as_csv(df, "resale_flat_prices")


def _load_mrt_stations():
    log.info(f"Loading {mrt_stations_filename} from staging into storage")

    source = Path(staging_path) / mrt_stations_filename
    df = pd.read_excel(source, sheet_name="Sheet1")

    _store_as_csv(df, "mrt_stations")


def _load_mrt_geodata():
    log.info(f"Loading {mrt_geodata_filename} from staging into storage")

    source = Path(staging_path) / mrt_geodata_filename
    df = _load_json_dataframe(source)

    _store_as_csv(df, "mrt_geodata")


def _load_mall_geodata():
    log.info(f"Loading {mall_geodata_filename} from staging into storage")

    source = Path(staging_path) / mall_geodata_filename
    df = _load_json_dataframe(source)

    _store_as_csv(df, "mall_geodata")


def _load_json_dataframe(source: Path):
    with source.open(encoding="utf-8") as f:
        data = json.load(f)

    data_dict = pd.json_normalize(data, record_path=["elements"])
    return pd.DataFrame.from_dict(data_dict, orient="columns")  # type: ignore


def _store_as_parquet(df: pd.DataFrame, filename: str) -> None:
    destination = Path(storage_path) / (filename + ".parquet")
    df.to_parquet(destination)


def _store_as_csv(df: pd.DataFrame, filename: str) -> None:
    destination = Path(storage_path) / (filename + ".csv")
    df.to_csv(destination)

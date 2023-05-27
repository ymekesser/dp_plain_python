import json
from pathlib import Path
import pandas as pd
import logging
from dp_plain_python.environment import config, file_storage

log = logging.getLogger(__name__)

staging_path = config.get_location("Staging")
storage_path = config.get_location("Storage")

resale_flat_prices_filename = config.get_sourcefile_path("ResaleFlatPrices").name
mrt_stations_filename = config.get_sourcefile_path("MrtStations").name
mrt_geodata_filename = "mrt_geodata.json"
mall_geodata_filename = "mall_geodata.json"
address_geodata_filename = config.get_sourcefile_path("HdbAddressGeodata").name

storage = file_storage.LocalFileStorage()


def load_into_storage():
    log.info(f"Starting Loading Step.")

    storage.ensure_directory(storage_path)

    _load_resale_flat_prices()
    _load_mrt_stations()
    _load_mrt_geodata()
    _load_mall_geodata()
    _load_address_geodata()


def _load_resale_flat_prices():
    log.info(f"Loading {resale_flat_prices_filename} from staging into storage")

    source = staging_path / resale_flat_prices_filename

    df = storage.read_dataframe(source)

    _store_as_csv(df, "resale_flat_prices")


def _load_mrt_stations():
    log.info(f"Loading {mrt_stations_filename} from staging into storage")

    source = staging_path / mrt_stations_filename
    df = pd.read_excel(source, sheet_name="Sheet1")

    _store_as_csv(df, "mrt_stations")


def _load_mrt_geodata():
    log.info(f"Loading {mrt_geodata_filename} from staging into storage")

    source = staging_path / mrt_geodata_filename
    df = _load_overpass_json_dataframe(source)

    _store_as_csv(df, "mrt_geodata")


def _load_mall_geodata():
    log.info(f"Loading {mall_geodata_filename} from staging into storage")

    source = staging_path / mall_geodata_filename
    df = _load_overpass_json_dataframe(source)

    _store_as_csv(df, "mall_geodata")


def _load_address_geodata():
    log.info(f"Loading {address_geodata_filename} from staging into storage")

    source = staging_path / address_geodata_filename

    df = pd.read_csv(source)

    _store_as_csv(df, "address_geodata")


def _load_overpass_json_dataframe(source: Path):
    with source.open(encoding="utf-8") as f:
        data = json.load(f)

    data_dict = pd.json_normalize(data, record_path=["elements"])
    return pd.DataFrame.from_dict(data_dict, orient="columns")  # type: ignore


def _store_as_csv(df: pd.DataFrame, filename: str) -> None:
    destination = storage_path / (filename + ".csv")
    storage.write_dataframe(df, destination)

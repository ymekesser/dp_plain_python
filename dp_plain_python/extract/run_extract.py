import logging
from typing import Any
import pandas as pd
from os import makedirs
import shutil
from pathlib import Path
import json

from dp_plain_python.extract.geolocation import (
    get_shopping_malls_geodata,
    get_mrt_stations_geodata,
)

log = logging.getLogger(__name__)

# todo: Make configurable
resale_flat_prices_path = (
    "..\\data\\resale-flat-prices-based-on-registration-date-from-jan-2017-onwards.csv"
)
mrt_stations_path = "..\\data\\mrt_stations.xlsx"

staging_path = "local_data\\staging"


def extract_into_staging() -> None:
    log.info(f"Starting Extraction Step.")

    makedirs(staging_path, exist_ok=True)

    _extract_resale_flat_prices()
    _extract_mrt_stations()
    _extract_mrt_geodata()
    _extract_mall_geodata()


def _extract_resale_flat_prices() -> None:
    log.info("Extracting hdb resale flat prices data")
    _extract_file(resale_flat_prices_path)


def _extract_mrt_stations() -> None:
    log.info("Extracting MRT station data")
    _extract_file(mrt_stations_path)


def _extract_mrt_geodata() -> None:
    log.info("Extracting MRT geodata")
    data = get_mrt_stations_geodata()

    _save_as_json(data, "mrt_geodata.json")


def _extract_mall_geodata() -> None:
    log.info("Extracting Shopping Mall geodata")
    data = get_shopping_malls_geodata()

    _save_as_json(data, "mall_geodata.json")


def _save_as_json(data: Any, filename: str) -> None:
    dst = Path(staging_path) / filename

    log.info(f"Saving JSON data to to {dst}")

    with open(dst, "w") as f:
        json.dump(data, f)


def _extract_file(path: str) -> None:
    src = Path(path)
    file_name = src.name
    dst = Path(staging_path) / file_name

    log.info(f"Copying file {src} to {dst}")

    shutil.copy(src, dst)

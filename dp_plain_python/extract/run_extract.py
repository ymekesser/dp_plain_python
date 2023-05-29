import logging
from dp_plain_python.environment import config, file_storage

from dp_plain_python.extract.geolocation import (
    get_shopping_malls_geodata,
    get_mrt_stations_geodata,
)

log = logging.getLogger(__name__)

resale_flat_prices_path = config.get_sourcefile_path("ResaleFlatPrices")
mrt_stations_path = config.get_sourcefile_path("MrtStations")
address_geodata_path = config.get_sourcefile_path("HdbAddressGeodata")

staging_path = config.get_location("Staging")

storage = file_storage.get_storage()


def extract_into_staging() -> None:
    log.info(f"Starting Extraction Step.")

    storage.ensure_directory(staging_path)

    _extract_resale_flat_prices()
    _extract_mrt_stations()
    _extract_mrt_geodata()
    _extract_mall_geodata()
    _extract_address_geodata()


def _extract_resale_flat_prices() -> None:
    log.info("Extracting hdb resale flat prices data")

    storage.copy_file(resale_flat_prices_path, staging_path)


def _extract_mrt_stations() -> None:
    log.info("Extracting MRT station data")

    storage.copy_file(mrt_stations_path, staging_path)


def _extract_mrt_geodata() -> None:
    log.info("Extracting MRT geodata")
    data = get_mrt_stations_geodata()

    storage.write_json(data, staging_path / "mrt_geodata.json")


def _extract_mall_geodata() -> None:
    log.info("Extracting Shopping Mall geodata")
    data = get_shopping_malls_geodata()

    storage.write_json(data, staging_path / "mall_geodata.json")


def _extract_address_geodata() -> None:
    log.info("Extracting address geolocation data")

    storage.copy_file(address_geodata_path, staging_path)

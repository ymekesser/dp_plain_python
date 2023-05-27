import configparser
from pathlib import Path
from typing import Literal

_config = configparser.ConfigParser()
_config.read("./config.ini")

_config_section_locations = "LOCATIONS"
_config_section_sourcefile_paths = "SOURCEFILE_PATHS"
_config_section_storage_filenames = "STORAGE_FILENAMES"

_location = Literal["Staging", "Storage", "TransformedAnalytics", "Analytics"]
_sourcefiles = Literal["ResaleFlatPrices", "MrtStations", "HdbAddressGeodata"]
_storage_file = Literal[
    "ResaleFlatPrices",
    "MrtStations",
    "MrtGeodata",
    "MallGeodata",
    "HdbAddressGeodata",
]


def get_location(location: _location) -> Path:
    path = _config.get(_config_section_locations, location)
    return Path(path)


def get_sourcefile_path(sourcefile: _sourcefiles) -> Path:
    path = _config.get(_config_section_sourcefile_paths, sourcefile)
    return Path(path)


def get_storage_filename(file: _storage_file) -> Path:
    path = _config.get(_config_section_storage_filenames, file)
    return Path(path)

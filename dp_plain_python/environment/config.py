import configparser
from pathlib import Path
from typing import Literal

_config = configparser.ConfigParser()
_config.read("./config.ini")

_config_section_locations = "LOCATIONS"
_config_section_sourcefile_paths = "SOURCEFILE_PATHS"

_location = Literal["Staging", "Storage", "TransformedAnalytics"]
_sourcefiles = Literal["ResaleFlatPrices", "MrtStations", "HdbAddressGeodata"]


def get_location(location: _location) -> Path:
    path = _config.get(_config_section_locations, location)
    return Path(path)


def get_sourcefile_path(sourcefile: _sourcefiles) -> Path:
    path = _config.get(_config_section_sourcefile_paths, sourcefile)
    return Path(path)

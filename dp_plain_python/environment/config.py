import configparser
from pathlib import Path
import os
from typing import Literal

_config_path = Path(os.getcwd()) / "config.ini"
_config = configparser.ConfigParser()
_config.read(_config_path)

_config_section_locations = "LOCATIONS"
_config_section_sourcefile_paths = "SOURCEFILE_PATHS"
_config_section_storage_filenames = "STORAGE_FILENAMES"
_config_section_logging = "LOGGING"
_config_section_pipeline = "PIPELINE"
_config_section_api_endpoints = "API_ENDPOINTS"
_config_section_file_access = "FILE_ACCESS"

_location = Literal["Staging", "Storage", "TransformedAnalytics", "Analytics"]
_sourcefiles = Literal["ResaleFlatPrices", "MrtStations", "HdbAddressGeodata"]
_storage_file = Literal[
    "ResaleFlatPrices",
    "MrtStations",
    "MrtGeodata",
    "MallGeodata",
    "HdbAddressGeodata",
]
_logging = Literal["Enabled", "Level"]
_pipeline = Literal[
    "ExtractEnabled",
    "LoadEnabled",
    "TransformAnalyticsEnabled",
    "AnalyticsEnabled",
]
_endpoint = Literal["Overpass"]
_file_access = Literal["Mode", "S3Bucket"]


def get_location(location: _location) -> Path:
    path = _config.get(_config_section_locations, location)
    return Path(path)


def get_file_access_setting(setting: _file_access):
    return _config.get(_config_section_file_access, setting)


def get_sourcefile_path(sourcefile: _sourcefiles) -> Path:
    path = _config.get(_config_section_sourcefile_paths, sourcefile)
    return Path(path)


def get_storage_filename(file: _storage_file) -> Path:
    path = _config.get(_config_section_storage_filenames, file)
    return Path(path)


def get_logging_setting(setting: _logging) -> str:
    return _config.get(_config_section_logging, setting)


def get_pipeline_setting(setting: _pipeline) -> str:
    return _config.get(_config_section_pipeline, setting)


def get_api_endpoint(endpoint: _endpoint) -> str:
    return _config.get(_config_section_api_endpoints, endpoint)

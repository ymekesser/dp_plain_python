import abc
import json
import shutil
from typing import Any, Literal, Union
import pandas as pd
from os import makedirs
from pathlib import Path
from pandas import DataFrame
import logging

log = logging.getLogger(__name__)


class FileStorage(abc.ABC):
    @abc.abstractmethod
    def write_dataframe(self, dataframe: DataFrame, path: str) -> None:
        pass

    @abc.abstractmethod
    def read_dataframe(self, path: str) -> DataFrame:
        pass


class LocalFileStorage(FileStorage):
    def __init__(self, base_path: Path = Path(".")) -> None:
        self._path_prefix = Path(base_path)

    def ensure_directory(self, path: Path) -> None:
        log.info(f"Ensure directory {path}")
        makedirs(path, exist_ok=True)

    def write_dataframe(self, dataframe: DataFrame, path: Path) -> None:
        log.info(f"Write dataframe to {path}")
        full_path = self._path_prefix / path
        makedirs(full_path.parent, exist_ok=True)

        dataframe.to_csv(full_path, lineterminator="\n")

    def read_dataframe(self, path: Path) -> DataFrame:
        log.info(f"Read dataframe from {path}")
        return pd.read_csv(self._path_prefix / path)

    def write_json(self, data: Any, path: Path):
        log.info(f"Writing JSON data to {path}")

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

    def copy_file(
        self,
        src_path: Path,
        dst_path: Path,
    ) -> None:
        src = Path(src_path)
        dst = Path(dst_path)

        if dst_path.is_dir():
            dst = Path(dst_path) / src.name

        log.info(f"Copying file {src} to {dst}")

        shutil.copy(src, dst)


# todo
# class S3FileStorage(FileStorage):

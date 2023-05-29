import abc
import json
import shutil
import tempfile
from typing import Any, Union
import pandas as pd
from os import makedirs
from pathlib import Path
from pandas import DataFrame
import pickle
import logging
import boto3
from dp_plain_python.environment import config

log = logging.getLogger(__name__)


class FileStorage(abc.ABC):
    @abc.abstractmethod
    def ensure_directory(self, path: Union[Path, str]) -> None:
        pass

    @abc.abstractmethod
    def write_dataframe(self, dataframe: DataFrame, path: Union[Path, str]) -> None:
        pass

    @abc.abstractmethod
    def read_dataframe(self, path: Union[Path, str]) -> DataFrame:
        pass

    @abc.abstractmethod
    def read_json(self, path: Union[Path, str]) -> Any:
        pass

    @abc.abstractmethod
    def write_json(self, data: Any, path: Union[Path, str]):
        pass

    @abc.abstractmethod
    def read_excel(self, path: Union[Path, str], sheet: str) -> DataFrame:
        pass

    @abc.abstractmethod
    def write_pickle(self, object: Any, path: Union[Path, str]) -> None:
        pass

    @abc.abstractmethod
    def copy_file(
        self,
        src_path: Union[Path, str],
        dst_path: Union[Path, str],
    ) -> None:
        pass


class LocalFileStorage(FileStorage):
    def __init__(self) -> None:
        super().__init__()

    def ensure_directory(self, path: Union[Path, str]) -> None:
        log.info(f"Ensure directory {path}")
        makedirs(path, exist_ok=True)

    def write_dataframe(self, dataframe: DataFrame, path: Union[Path, str]) -> None:
        log.info(f"Write dataframe to {path}")
        dataframe.to_csv(path, lineterminator="\n")

    def read_dataframe(self, path: Union[Path, str]) -> DataFrame:
        log.info(f"Read dataframe from {path}")
        return pd.read_csv(path)

    def read_json(self, path: Union[Path, str]) -> Any:
        log.info(f"Read JSON data from {path}")

        with open(path, encoding="utf-8") as f:
            data = json.load(f)

        return data

    def write_json(self, data: Any, path: Union[Path, str]):
        log.info(f"Writing JSON data to {path}")

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

    def read_excel(self, path: Union[Path, str], sheet: str) -> DataFrame:
        log.info(f"Read excel data from {path} (sheet: {sheet})")
        return pd.read_excel(path, sheet_name=sheet)

    def write_pickle(self, object: Any, path: Union[Path, str]) -> None:
        log.info(f"Writing pickled object to {path}")
        pickle.dump(object, open(path, "wb"))

    def copy_file(
        self,
        src_path: Union[Path, str],
        dst_path: Union[Path, str],
    ) -> None:
        src = Path(src_path)
        dst = Path(dst_path)

        if dst.is_dir():
            dst = Path(dst_path) / src.name

        log.info(f"Copying file {src} to {dst}")

        shutil.copy(src, dst)


class S3Storage(FileStorage):
    def __init__(self, bucket_name: str) -> None:
        self._tmp_dir = Path(tempfile.gettempdir())

        self._bucket_name = bucket_name
        self._s3_client = boto3.client("s3")

    def ensure_directory(self, _: Union[Path, str]) -> None:
        pass

    def read_dataframe(self, path: Union[Path, str]) -> DataFrame:
        log.info(f"Read dataframe from {path}")
        tmp_path = self._get_temp_file_path()

        self._s3_client.download_file(self._bucket_name, _s3_path(path), tmp_path)

        return pd.read_csv(tmp_path)

    def read_json(self, path: Union[Path, str]) -> Any:
        log.info(f"Read JSON data from {path}")
        tmp_path = self._get_temp_file_path()

        self._s3_client.download_file(self._bucket_name, _s3_path(path), tmp_path)

        with open(tmp_path, encoding="utf-8") as f:
            data = json.load(f)

        return data

    def write_dataframe(self, dataframe: DataFrame, path: Union[Path, str]) -> None:
        log.info(f"Write dataframe to {path}")
        tmp_path = self._get_temp_file_path()

        dataframe.to_csv(tmp_path, lineterminator="\n")

        self._upload(tmp_path, path)

    def write_json(self, data: Any, path: Union[Path, str]):
        log.info(f"Writing JSON data to {path}")
        tmp_path = self._get_temp_file_path()

        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

        self._upload(tmp_path, path)

    def read_excel(self, path: Union[Path, str], sheet: str) -> DataFrame:
        log.info(f"Read excel data from {path} (sheet: {sheet})")
        tmp_path = self._get_temp_file_path()

        self._s3_client.download_file(self._bucket_name, _s3_path(path), tmp_path)

        return pd.read_excel(tmp_path, sheet_name=sheet)

    def write_pickle(self, object: Any, path: Union[Path, str]) -> None:
        log.info(f"Writing pickled object to {path}")
        tmp_path = self._get_temp_file_path()

        pickle.dump(object, open(tmp_path, "wb"))

        self._upload(tmp_path, path)

    def copy_file(
        self,
        src_path: Union[Path, str],
        dst_path: Union[Path, str],
    ) -> None:
        src = Path(src_path)
        dst = Path(dst_path)

        if dst.is_dir():
            dst = Path(dst_path) / src.name

        log.info(f"Copying file {src} to {dst}")

        copy_source = {"Bucket": self._bucket_name, "Key": _s3_path(src)}
        self._s3_client.copy_object(
            CopySource=copy_source,
            Bucket=self._bucket_name,  # Destination bucket
            Key=_s3_path(dst),  # Destination path/filename
        )

    def _get_temp_file_path(self) -> Path:
        return self._tmp_dir / "tmp.file"

    def _upload(self, tmp_path, dst_path) -> None:
        with open(tmp_path, "rb") as f:
            self._s3_client.upload_fileobj(f, self._bucket_name, _s3_path(dst_path))


def _s3_path(path: Union[Path, str]) -> str:
    if isinstance(path, str):
        return path
    else:
        return path.as_posix()


def get_storage() -> FileStorage:
    mode = config.get_file_access_setting("Mode")

    if mode == "Local":
        return LocalFileStorage()
    if mode == "S3":
        bucket_name = config.get_file_access_setting("S3Bucket")
        return S3Storage(bucket_name)
    else:
        raise ValueError(
            f"{mode} is not a supported file access mode setting. Use 'Local' or 'S3'."
        )

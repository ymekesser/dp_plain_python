import pytest
import pandas as pd
from unittest.mock import patch, Mock

from .file_storage import LocalFileStorage


base_path = "C:/foo"
test_data = {
    "Name": ["Alice", "Bob", "Charlie", "David"],
    "Age": [25, 30, 35, 40],
    "Gender": ["Female", "Male", "Male", "Male"],
    "Salary": [50000, 60000, 70000, 80000],
}


# @pytest.fixture(scope="module")
# def setup():
#     return "foo"


@patch("pandas.read_csv")
def test_local_file_storage_read_dataframe(read_csv_mock: Mock):
    lfs = LocalFileStorage(base_path)
    test_df = pd.DataFrame(test_data)
    read_csv_mock.return_value = test_df

    result = lfs.read_dataframe("bar.csv")

    read_csv_mock.assert_called_once()
    pd.testing.assert_frame_equal(result, test_df)


@patch("os.makedirs")
@patch("pandas.DataFrame.to_csv")
def test_local_file_storage_write_dataframe(makedirs_mock: Mock, to_csv_mock: Mock):
    lfs = LocalFileStorage(base_path)
    df = pd.DataFrame(test_data)
    to_csv_mock.return_value = df

    lfs.write_dataframe(df, "bar.csv")

    to_csv_mock.assert_called_once()
    to_csv_mock.assert_called_once_with(df)

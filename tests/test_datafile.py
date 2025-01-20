import pytest
import os
from tempfile import TemporaryDirectory
from pathlib import Path
from src import DataFile


@pytest.fixture
def tempFile():
    """Creates a temporary file for testing and ensures cleanup."""
    with TemporaryDirectory() as tempDir:
        tempFilePath = Path(tempDir) / "test_datafile.bin"
        yield str(tempFilePath)


@pytest.fixture
def sampleData():
    """Provides sample key, value, and timestamp."""
    return {"key": b"Sample Key", "value": b"Sample Value", "timestamp": 1122334455}


def test_invalid_file_path():
    """Test invalid file paths."""
    with pytest.raises(Exception):
        df = DataFile("/invalid/path/to/file", appendMode=True)
        df.open()


def test_file_access_permission(tempFile):
    """Test read-only file behavior."""
    # set read-ony permission
    with open(tempFile, "wb") as f:
        f.write(b"data")
    os.chmod(tempFile, 0o444)

    df = DataFile(tempFile, appendMode=True)
    with pytest.raises(Exception):
        df.open()


def test_reopening_file(tempFile):
    """Test reopening the same file."""
    df = DataFile(tempFile, appendMode=True)
    df.open()
    df.close()

    df.open()
    df.close()


def test_close_unopened_file(tempFile):
    """Test closing an unopened file."""
    df = DataFile(tempFile, appendMode=True)
    df.close()


def test_append_without_open(tempFile, sampleData):
    """Test appending without opening the file."""
    df = DataFile(tempFile, appendMode=True)

    with pytest.raises(Exception):
        df.appendRecord(**sampleData)


def test_append_empty_key_value(tempFile):
    """Test appending empty key and value."""
    df = DataFile(tempFile, appendMode=True)
    df.open()

    offset, length = df.appendRecord(key=b"a", value=b"", timestamp=123)
    assert offset >= 0
    assert length > 0

    df.close()


def test_append_negative_timestamp(tempFile):
    """Test appending with a negative timestamp."""
    df = DataFile(tempFile, appendMode=True)
    df.open()

    with pytest.raises(Exception):
        df.appendRecord(key=b"key", value=b"value", timestamp=-123)

    df.close()


def test_read_without_open(tempFile, sampleData):
    """Test reading without opening the file."""
    df = DataFile(tempFile, appendMode=False)

    with pytest.raises(Exception):
        df.readRecord(offset=0, length=10)


def test_read_invalid_offset_length(tempFile, sampleData):
    """Test reading with invalid offset and length."""
    df = DataFile(tempFile, appendMode=True)
    df.open()
    offset, length = df.appendRecord(**sampleData)
    df.close()

    df = DataFile(tempFile, appendMode=False)
    df.open()

    # negative offset
    with pytest.raises(Exception):
        df.readRecord(offset=-1, length=length)

    # negative length
    with pytest.raises(Exception):
        df.readRecord(offset=offset, length=-1)

    # exceeding file size
    with pytest.raises(Exception):
        df.readRecord(offset=offset, length=1000000)

    df.close()


def test_read_partial_record(tempFile, sampleData):
    """Test reading a record with incomplete data."""
    df = DataFile(tempFile, appendMode=True)
    df.open()
    offset, length = df.appendRecord(**sampleData)
    df.close()

    # truncate file
    with open(tempFile, "r+b") as f:
        f.truncate(length // 2)

    df = DataFile(tempFile, appendMode=False)
    df.open()

    with pytest.raises(Exception):
        df.readRecord(offset=offset, length=length)

    df.close()


def test_read_valid_record(tempFile, sampleData):
    """Test reading a valid record."""
    df = DataFile(tempFile, appendMode=True)
    df.open()
    offset, length = df.appendRecord(**sampleData)
    df.close()

    df = DataFile(tempFile, appendMode=False)
    df.open()
    key, value = df.readRecord(offset=offset, length=length)
    assert key == sampleData["key"]
    assert value == sampleData["value"]
    df.close()


def test_open_close_lifecycle(tempFile):
    """Test repeated open-close cycles."""
    df = DataFile(tempFile, appendMode=True)

    for _ in range(5):
        df.open()
        df.close()


def test_append_read_lifecycle(tempFile, sampleData):
    """Test appending and reading in sequence."""
    df = DataFile(tempFile, appendMode=True)
    df.open()
    offset, length = df.appendRecord(**sampleData)
    df.close()

    df = DataFile(tempFile, appendMode=False)
    df.open()
    key, value = df.readRecord(offset=offset, length=length)
    assert key == sampleData["key"]
    assert value == sampleData["value"]
    df.close()


def test_large_records(tempFile):
    """Test appending and reading large records."""
    large_key = b"a" * (2**10)  # 1 kb key
    large_value = b"b" * (2**20)  # 1 mb value
    df = DataFile(tempFile, appendMode=True)
    df.open()
    offset, length = df.appendRecord(key=large_key, value=large_value, timestamp=123)
    df.close()

    df = DataFile(tempFile, appendMode=False)
    df.open()
    key, value = df.readRecord(offset=offset, length=length)
    assert key == large_key
    assert value == large_value
    df.close()

from typing import IO, Any, Optional, Tuple, BinaryIO
import struct


class DataFile:
    """
    Represents an immutable data file for storing key-value pairs in an append-only manner.

    Attributes
    ----------
    appendMode : bool
        Whether the file is opened for appending (True) or reading (False).
    """

    def __init__(self, filePath: str, appendMode: bool):
        """
        Initializes a DataFile instance.

        Parameters
        ----------
        filePath : str
            Path to the data file.
        appendMode : bool
            True for appending, False for reading.

        Raises
        ------
        ValueError
            If filePath is empty.
        TypeError
            If appendMode is not a boolean.
        """
        if not filePath:
            raise ValueError("filePath Cannot Be Empty.")
        if not isinstance(appendMode, bool):
            raise TypeError("appendMode Must Be A Boolean.")

        self._filePath = filePath
        self.appendMode = appendMode

        self._fileHandle: Optional[IO[Any]] = None
        self._lastReadOffset: int = 0
        self._fileSize: int = 0

    def open(self) -> None:
        """
        Opens the data file with the specified mode.

        Raises
        ------
        IOError
            If the file cannot be opened.
        """
        mode = "ab" if self.appendMode else "rb"

        try:
            self._fileHandle = open(self._filePath, mode)
            self._fileSize = self._fileHandle.seek(0, 2)

        except OSError as e:
            raise IOError(f"Failed To Open File {self._filePath}: {e}")

    def appendRecord(self, key: bytes, value: bytes, timestamp: int) -> Tuple[int, int]:
        """
        Appends a key-value pair to the data file.

        Parameters
        ----------
        key : bytes
            The key to store.
        value : bytes
            The value to store.
        timestamp : int
            The timestamp of the operation.

        Returns
        -------
        Tuple[int, int]
            The byte offset and length of the written record.

        Raises
        ------
        RuntimeError
            If the file is not open for appending.
        ValueError
            If key is empty or not a bytes object, or timestamp is negative.
        """
        if self._fileHandle is None or self._fileHandle.closed:
            raise RuntimeError("File Is Not Open For Appending.")
        if not key or not isinstance(key, bytes):
            raise ValueError("Key Must Be A Non-Empty Bytes Object.")
        if timestamp < 0:
            raise ValueError("Timestamp Must Be Non-Negative.")

        keySize, valueSize = len(key), len(value)
        recordFormat = f"!Q I I {keySize}s {valueSize}s"
        record = struct.pack(
            recordFormat,
            timestamp,
            keySize,
            valueSize,
            key,
            value,
        )

        offset = self._fileHandle.tell()
        self._fileHandle.write(record)
        self._fileHandle.flush()
        self._fileSize = self._fileHandle.tell()

        return offset, len(record)

    def readRecord(self, offset: int, length: int) -> Tuple[bytes, bytes]:
        """
        Reads a key-value pair from the data file.

        Parameters
        ----------
        offset : int
            Byte offset where the record starts.
        length : int
            Length of the record to read.

        Returns
        -------
        Tuple[bytes, bytes]
            The key and value stored in the record.

        Raises
        ------
        RuntimeError
            If the file is not open for reading.
        ValueError
            If offset or length is negative.
        IndexError
            If the record exceeds the file size or is incomplete.
        """
        if self._fileHandle is None or self._fileHandle.closed:
            raise RuntimeError("File Is Not Open For Reading.")
        if offset < 0 or length < 0:
            raise ValueError("Offset And Length Must Be Non-Negative.")
        if offset > self._fileSize or length > self._fileSize - offset:
            raise IndexError("Record Exceeds File Size.")

        self._fileHandle.seek(offset)
        recordData = self._fileHandle.read(length)

        headerFormat = "!Q I I"
        headerSize = struct.calcsize(headerFormat)

        if len(recordData) < headerSize:
            raise IndexError("Record Data Doesn't Include Header.")

        _, keySize, valueSize = struct.unpack(headerFormat, recordData[:headerSize])

        keyStart = headerSize
        keyEnd = keyStart + keySize
        valueStart = keyEnd
        valueEnd = valueStart + valueSize

        if valueEnd > len(recordData):
            raise IndexError("Record Data Doesn't Include Full Key-Value Pair.")

        key = recordData[keyStart:keyEnd]
        value = recordData[valueStart:valueEnd]

        return key, value

    def size(self) -> int:
        """
        Returns the size of the data file in bytes.

        Returns
        -------
        int
            The size of the data file in bytes.
        """
        return self._fileSize

    def close(self) -> None:
        """
        Closes the data file.
        """
        if self._fileHandle:
            self._fileHandle.close()
            self._fileHandle = None

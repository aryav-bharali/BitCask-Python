from typing import Optional, Tuple, BinaryIO
import struct


class DataFile:
    """
    Represents an immutable data file for storing key-value pairs in an append-only manner.

    Attributes
    ----------
    filePath : str
        Path to the data file on disk.
    appendMode : bool
        Whether the file is opened for appending (True) or reading (False).
    fileHandle : Optional[object]
        File handle for low-level I/O operations.
    lastReadOffset : int
        The last read position in the file, used for optimizing sequential reads.
    fileSize : int
        The size of the file in bytes.
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
        """
        self.filePath = filePath
        self.appendMode = appendMode
        self.fileHandle: Optional[BinaryIO] = None
        self.lastReadOffset: int = 0
        self.fileSize: int = 0

    def open(self) -> None:
        """
        Opens the data file with the specified mode.

        Raises
        ------
        Exception
            If the file cannot be opened.
        """
        mode = "ab" if self.appendMode else "rb"

        try:
            self.fileHandle = open(self.filePath, mode)

            self.fileHandle.seek(0, 2)
            self.fileSize = self.fileHandle.tell()

        except OSError as e:
            raise Exception(f"Failed To Open File: {e}")

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
        """
        if self.fileHandle is None or self.fileHandle.closed:
            raise Exception("File Is Not Open For Appending")
        elif timestamp < 0:
            raise Exception("Timestamp Must Be Non-Negative")

        # Serialize The Record
        key_size = len(key)
        value_size = len(value)
        record_format = f"!I Q I I {key_size}s {value_size}s"
        record = struct.pack(
            record_format,
            key_size + value_size + 16,
            timestamp,
            key_size,
            value_size,
            key,
            value,
        )

        # Write The Record
        offset = self.fileHandle.tell()
        self.fileHandle.write(record)
        self.fileHandle.flush()

        # Update fileSize directly using tell()
        self.fileSize = self.fileHandle.tell()

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
        """
        if self.fileHandle is None or self.fileHandle.closed:
            raise Exception("File Is Not Open For Reading")
        elif offset < 0 or length < 0:
            raise Exception("Offset And Length Must Be Non-Negative")

        # Efficient bounds checking
        if offset > self.fileSize or length > self.fileSize - offset:
            raise Exception("Record Exceeds File Size")

        # Absolute Seek to the offset
        self.fileHandle.seek(offset)

        # Read & Parse The Record
        record_data = self.fileHandle.read(length)

        # Extract Header & Key-Value Data
        header_format = "!I Q I I"
        header_size = struct.calcsize(header_format)

        if len(record_data) < header_size:
            raise Exception("Incomplete Record Data")

        _, _, key_size, value_size = struct.unpack(
            header_format, record_data[:header_size]
        )

        key_start = header_size
        key_end = key_start + key_size
        value_start = key_end
        value_end = value_start + value_size

        key = record_data[key_start:key_end]
        value = record_data[value_start:value_end]

        return key, value

    def close(self) -> None:
        """
        Closes the data file.
        """
        if self.fileHandle:
            self.fileHandle.close()
            self.fileHandle = None

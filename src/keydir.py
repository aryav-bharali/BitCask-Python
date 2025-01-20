from typing import Dict, Optional
from collections import namedtuple


KeyDirEntry = namedtuple(
    "KeyDirEntry", ["fileID", "valueSize", "valuePos", "timestamp"]
)


class KeyDir:
    """
    Represents the in-memory index for mapping keys to file locations.
    """

    def __init__(self):
        """
        Initializes an empty KeyDir instance.
        """
        self._index: Dict[bytes, KeyDirEntry] = {}

    def addEntry(
        self, key: bytes, fileID: int, valueSize: int, valuePos: int, timestamp: int
    ) -> None:
        """
        Adds or updates an entry in the key directory.

        Parameters
        ----------
        key : bytes
            The key to store.
        fileID : int
            The ID of the file where the key-value pair is stored.
        valueSize : int
            The size of the value.
        valuePos : int
            The position (offset) of the value in the file.
        timestamp : int
            The timestamp of the operation.

        Raises
        ------
        ValueError
            If the key is empty or any of the values are negative

        Notes
        -----
        If the key already exists, its entry will be updated with the new metadata.
        """
        if not key or not isinstance(key, bytes):
            raise ValueError("Key Must Be A Non-Empty Bytes Object.")
        if fileID < 0 or valueSize < 0 or valuePos < 0:
            raise ValueError("FileID, ValueSize, And ValuePos Must Be Non-Negative.")
        
        self._index[key] = KeyDirEntry(fileID, valueSize, valuePos, timestamp)

    def getEntry(self, key: bytes) -> Optional[KeyDirEntry]:
        """
        Retrieves the metadata for a given key.

        Parameters
        ----------
        key : bytes
            The key to retrieve.

        Raises
        ------
        ValueError
            If the key is empty or not a bytes object.

        Returns
        -------
        Optional[KeyDirEntry]
            Metadata for the key, or None if the key is not found.
        """
        if not key or not isinstance(key, bytes):
            raise ValueError("Key Must Be A Non-Empty Bytes Object.")

        return self._index.get(key)

    def removeEntry(self, key: bytes) -> None:
        """
        Removes a key from the directory.

        Parameters
        ----------
        key : bytes
            The key to remove.

        Raises
        ------
        ValueError
            If the key is empty or not a bytes object.
        """
        if not key or not isinstance(key, bytes):
            raise ValueError("Key Must Be A Non-Empty Bytes Object.")
        
        if key in self._index:
            del self._index[key]
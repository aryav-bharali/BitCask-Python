import pytest
from src import KeyDir, KeyDirEntry

TEST_ENTRY = KeyDirEntry(1, 100, 10, 123456)


@pytest.fixture
def keyDir():
    """Fixture to initialize a fresh KeyDir instance for each test."""
    return KeyDir()


def assertEntry(keydir, key, expected):
    """Helper to assert that a key's metadata matches the expected values."""
    entry = keydir.getEntry(key)
    assert entry == expected


def test_add_new_key(keyDir):
    """Test adding a new key with valid inputs."""
    key = b"Test Key"
    keyDir.addEntry(key, *TEST_ENTRY)
    assertEntry(keyDir, key, TEST_ENTRY)


def test_update_existing_key(keyDir):
    """Test updating metadata of an existing key."""
    key = b"test_key"
    updatedEntry = KeyDirEntry(2, 200, 20, 123457)
    keyDir.addEntry(key, *TEST_ENTRY)
    keyDir.addEntry(key, *updatedEntry)
    assertEntry(keyDir, key, updatedEntry)


def test_add_empty_key(keyDir):
    """Test adding an empty key."""
    key = b""
    with pytest.raises(ValueError):
        keyDir.addEntry(key, *TEST_ENTRY)


@pytest.mark.parametrize("invalidKey", [None, "not_bytes", 123])
def test_add_invalid_key_type(keyDir, invalidKey):
    """Test adding keys with invalid types."""
    with pytest.raises(ValueError):
        keyDir.addEntry(invalidKey, *TEST_ENTRY)


def test_add_negative_values(keyDir):
    """Test adding entries with negative values."""
    key = b"Negative Test"
    with pytest.raises(ValueError):
        keyDir.addEntry(key, 1, -1, -10, -123456)


def test_add_zero_size_and_position(keyDir):
    """Test adding entries with zero size or position."""
    key = b"Zero Test"
    zeroEntry = KeyDirEntry(1, 0, 0, 123456)
    keyDir.addEntry(key, *zeroEntry)
    assertEntry(keyDir, key, zeroEntry)


def test_get_existing_key(keyDir):
    """Test retrieving metadata for an existing key."""
    key = b"Existing Key"
    keyDir.addEntry(key, *TEST_ENTRY)
    assertEntry(keyDir, key, TEST_ENTRY)


def test_get_nonexistent_key(keyDir):
    """Test retrieving a key that does not exist."""
    key = b"Non-Existent Key"
    assertEntry(keyDir, key, None)


@pytest.mark.parametrize("invalidKey", [None, "not_bytes", 123])
def test_get_invalid_key_type(keyDir, invalidKey):
    """Test retrieving keys with invalid types."""
    with pytest.raises(ValueError):
        keyDir.getEntry(invalidKey)


def test_get_removed_key(keyDir):
    """Test retrieving a key that has been removed."""
    key = b"To Be Removed"
    keyDir.addEntry(key, *TEST_ENTRY)
    assertEntry(keyDir, key, TEST_ENTRY)
    keyDir.removeEntry(key)
    assertEntry(keyDir, key, None)


def test_remove_nonexistent_key(keyDir):
    """Test removing a key that does not exist."""
    key = b"Non-Existent Key"
    keyDir.removeEntry(key)
    assertEntry(keyDir, key, None)


@pytest.mark.parametrize("invalidKey", [None, "not_bytes", 123])
def test_remove_invalid_key_type(keyDir, invalidKey):
    """Test removing keys with invalid types."""
    with pytest.raises(ValueError):
        keyDir.removeEntry(invalidKey)


def test_multiple_keys(keyDir):
    """Test adding, retrieving, and removing multiple keys."""
    keys = [b"key1", b"key2", b"key3"]
    entries = [
        KeyDirEntry(
            i,
            i * 100,
            i * 10,
            123456 + i,
        )
        for i in range(1, 4)
    ]

    for key, entry in zip(keys, entries):
        keyDir.addEntry(key, *entry)

    for key, entry in zip(keys, entries):
        assertEntry(keyDir, key, entry)

    for key in keys:
        keyDir.removeEntry(key)
        assertEntry(keyDir, key, None)


def test_multiple_operations_on_same_key(keyDir):
    """Test multiple operations (add, update, remove, add, update) on the same key."""
    key = b"MultiOp Key"
    firstEntry = KeyDirEntry(1, 100, 10, 123456)
    updatedEntry = KeyDirEntry(2, 200, 20, 123457)
    secondEntry = KeyDirEntry(3, 300, 30, 123458)
    finalEntry = KeyDirEntry(4, 400, 40, 123459)

    keyDir.addEntry(key, *firstEntry)
    assertEntry(keyDir, key, firstEntry)

    keyDir.addEntry(key, *updatedEntry)
    assertEntry(keyDir, key, updatedEntry)

    keyDir.removeEntry(key)
    assertEntry(keyDir, key, None)

    keyDir.addEntry(key, *secondEntry)
    assertEntry(keyDir, key, secondEntry)

    keyDir.addEntry(key, *finalEntry)
    assertEntry(keyDir, key, finalEntry)

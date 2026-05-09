"""Hash utility functions for file integrity checks."""

import hashlib
from typing import BinaryIO


def calculate_file_sha256(file_path: str, chunk_size: int = 4096) -> str:
    """
    Calculate SHA-256 checksum of a file.

    Args:
        file_path: Path to the file
        chunk_size: Size of chunks to read (default: 4096 bytes)

    Returns:
        SHA-256 hash as hexadecimal string

    Example:
        >>> hash_value = calculate_file_sha256("/path/to/file.csv")
        >>> print(hash_value)
        'a1b2c3d4e5f6...'
    """
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(chunk_size), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def calculate_content_sha256(content: bytes) -> str:
    """
    Calculate SHA-256 checksum of content in memory.

    Args:
        content: File content as bytes

    Returns:
        SHA-256 hash as hexadecimal string

    Example:
        >>> content = b"Hello, World!"
        >>> hash_value = calculate_content_sha256(content)
        >>> print(hash_value)
        'dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f'
    """
    return hashlib.sha256(content).hexdigest()


def calculate_stream_sha256(file_stream: BinaryIO, chunk_size: int = 4096) -> str:
    """
    Calculate SHA-256 checksum of a file stream.

    Useful for calculating hash while reading file without loading entire content.

    Args:
        file_stream: Binary file stream (file-like object)
        chunk_size: Size of chunks to read (default: 4096 bytes)

    Returns:
        SHA-256 hash as hexadecimal string
    """
    sha256_hash = hashlib.sha256()
    file_stream.seek(0)  # Ensure we start from beginning
    for byte_block in iter(lambda: file_stream.read(chunk_size), b""):
        sha256_hash.update(byte_block)
    file_stream.seek(0)  # Reset stream position
    return sha256_hash.hexdigest()

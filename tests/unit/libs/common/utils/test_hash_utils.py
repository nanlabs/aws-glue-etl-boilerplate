"""Unit tests for hash utility functions.

This module tests hash calculation functions for file integrity checks:
- calculate_file_sha256: Hash from file path
- calculate_content_sha256: Hash from bytes in memory
- calculate_stream_sha256: Hash from file stream

Test coverage includes:
- Success cases with known hash values
- Edge cases (empty files, large files, binary content)
- Error handling (missing files, invalid streams)
"""

import io
import os
import tempfile

import pytest

from libs.common.utils.hash_utils import (
    calculate_content_sha256,
    calculate_file_sha256,
    calculate_stream_sha256,
)


class TestCalculateContentSHA256:
    """Tests for calculate_content_sha256 function."""

    def test_known_content_hash(self):
        """Test hash calculation with known content."""
        content = b"Hello, World!"
        expected_hash = (
            "dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f"
        )

        result = calculate_content_sha256(content)

        assert result == expected_hash
        assert len(result) == 64  # SHA-256 hex digest length

    def test_empty_content(self):
        """Test hash calculation with empty content."""
        content = b""
        # SHA-256 of empty string
        expected_hash = (
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        )
        result = calculate_content_sha256(content)

        assert result == expected_hash

    def test_unicode_content(self):
        """Test hash calculation with UTF-8 encoded content."""
        content = "¡Hola, Mundo! 你好世界".encode("utf-8")
        result = calculate_content_sha256(content)

        # Verify hash format
        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)

    def test_binary_content(self):
        """Test hash calculation with binary content."""
        content = bytes(range(256))
        result = calculate_content_sha256(content)

        # Verify hash format
        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)

    def test_deterministic_hash(self):
        """Test that same content produces same hash (determinism)."""
        content = b"Test content for determinism"
        hash1 = calculate_content_sha256(content)
        hash2 = calculate_content_sha256(content)

        assert hash1 == hash2


class TestCalculateFileSHA256:
    """Tests for calculate_file_sha256 function."""

    def test_hash_from_file(self):
        """Test hash calculation from a file."""
        # Create temporary file
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            content = b"Test file content"
            f.write(content)
            temp_path = f.name

        try:
            result = calculate_file_sha256(temp_path)

            # Verify against content hash
            expected = calculate_content_sha256(content)
            assert result == expected
        finally:
            os.unlink(temp_path)

    def test_empty_file(self):
        """Test hash calculation from empty file."""
        # Create empty file
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            temp_path = f.name

        try:
            result = calculate_file_sha256(temp_path)

            # SHA-256 of empty content
            expected = (
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            )
            assert result == expected
        finally:
            os.unlink(temp_path)

    def test_large_file(self):
        """Test hash calculation with chunked reading (large file)."""
        # Create file with 10KB of data
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            large_content = b"x" * (10 * 1024)
            f.write(large_content)
            temp_path = f.name

        try:
            result = calculate_file_sha256(temp_path, chunk_size=1024)

            # Verify against content hash
            expected = calculate_content_sha256(large_content)
            assert result == expected
        finally:
            os.unlink(temp_path)

    def test_custom_chunk_size(self):
        """Test that custom chunk size produces same hash."""
        # Create test file
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            content = b"Chunk size test content" * 100
            f.write(content)
            temp_path = f.name

        try:
            # Test with different chunk sizes
            hash_default = calculate_file_sha256(temp_path)
            hash_small = calculate_file_sha256(temp_path, chunk_size=256)
            hash_large = calculate_file_sha256(temp_path, chunk_size=8192)

            # All should produce same hash
            assert hash_default == hash_small == hash_large
        finally:
            os.unlink(temp_path)

    def test_file_not_found(self):
        """Test error handling when file doesn't exist."""
        with pytest.raises(FileNotFoundError):
            calculate_file_sha256("/nonexistent/file/path.txt")

    def test_binary_file(self):
        """Test hash calculation from binary file."""
        # Create binary file
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            binary_content = bytes(range(256))
            f.write(binary_content)
            temp_path = f.name

        try:
            result = calculate_file_sha256(temp_path)

            # Verify against content hash
            expected = calculate_content_sha256(binary_content)
            assert result == expected
        finally:
            os.unlink(temp_path)


class TestCalculateStreamSHA256:
    """Tests for calculate_stream_sha256 function."""

    def test_hash_from_stream(self):
        """Test hash calculation from file stream."""
        content = b"Stream content test"
        stream = io.BytesIO(content)
        result = calculate_stream_sha256(stream)

        # Verify against content hash
        expected = calculate_content_sha256(content)
        assert result == expected

        # Verify stream position is reset to beginning
        assert stream.tell() == 0

    def test_empty_stream(self):
        """Test hash calculation from empty stream."""
        stream = io.BytesIO(b"")
        result = calculate_stream_sha256(stream)

        # SHA-256 of empty content
        expected = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        assert result == expected

    def test_stream_position_reset(self):
        """Test that stream position is reset before and after hashing."""
        content = b"Position reset test"
        stream = io.BytesIO(content)

        # Move stream to middle
        stream.seek(5)
        assert stream.tell() == 5

        # Calculate hash
        result = calculate_stream_sha256(stream)

        # Verify stream was reset to beginning
        assert stream.tell() == 0

        # Verify hash is correct (should hash entire content, not from position 5)
        expected = calculate_content_sha256(content)
        assert result == expected

    def test_large_stream(self):
        """Test hash calculation with chunked reading from stream."""
        large_content = b"y" * (10 * 1024)
        stream = io.BytesIO(large_content)
        result = calculate_stream_sha256(stream, chunk_size=1024)

        # Verify against content hash
        expected = calculate_content_sha256(large_content)
        assert result == expected

    def test_custom_chunk_size(self):
        """Test that custom chunk size produces same hash."""
        content = b"Chunk test" * 100
        stream1 = io.BytesIO(content)
        stream2 = io.BytesIO(content)
        stream3 = io.BytesIO(content)

        # Test with different chunk sizes
        hash_default = calculate_stream_sha256(stream1)
        hash_small = calculate_stream_sha256(stream2, chunk_size=256)
        hash_large = calculate_stream_sha256(stream3, chunk_size=8192)

        # All should produce same hash
        assert hash_default == hash_small == hash_large

    def test_real_file_stream(self):
        """Test hash calculation from real file object."""
        # Create temporary file
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            content = b"Real file stream test"
            f.write(content)
            temp_path = f.name

        try:
            # Open as file stream
            with open(temp_path, "rb") as file_stream:
                result = calculate_stream_sha256(file_stream)

                # Verify against content hash
                expected = calculate_content_sha256(content)
                assert result == expected

                # Verify stream position reset
                assert file_stream.tell() == 0
        finally:
            os.unlink(temp_path)


class TestHashFunctionsConsistency:
    """Tests for consistency between different hash functions."""

    def test_all_functions_produce_same_hash(self):
        """Test that all three functions produce the same hash for same content."""
        content = b"Consistency test content"

        # Create temporary file
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            f.write(content)
            temp_path = f.name

        try:
            # Calculate hash using all three methods
            hash_content = calculate_content_sha256(content)
            hash_file = calculate_file_sha256(temp_path)

            stream = io.BytesIO(content)
            hash_stream = calculate_stream_sha256(stream)

            # All should produce same hash
            assert hash_content == hash_file == hash_stream
        finally:
            os.unlink(temp_path)

    def test_csv_file_hash(self):
        """Test hashing CSV file (real-world use case from SFTP manifest)."""
        # Create sample CSV content similar to SFTP manifest files
        csv_content = """Name,Email,Phone
John Doe,john@example.com,555-0100
Jane Smith,jane@example.com,555-0101
""".encode("utf-8")

        # Create temporary CSV file
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Calculate hash from file
            file_hash = calculate_file_sha256(temp_path)

            # Calculate hash from content
            content_hash = calculate_content_sha256(csv_content)

            # Should match
            assert file_hash == content_hash

            # Verify hash format
            assert len(file_hash) == 64
        finally:
            os.unlink(temp_path)

# app/evidence/hashing.py
"""
Content-addressable hashing for evidence integrity.
All evidence is stored by SHA-256 hash of its content.
"""

import hashlib
from typing import Union


def compute_sha256(data: Union[bytes, str]) -> str:
    """
    Compute SHA-256 hash of data.

    Args:
        data: Raw bytes or string to hash

    Returns:
        Lowercase hex string of SHA-256 hash (64 characters)
    """
    if isinstance(data, str):
        data = data.encode('utf-8')
    return hashlib.sha256(data).hexdigest().lower()


def verify_sha256(data: Union[bytes, str], expected_hash: str) -> bool:
    """
    Verify that data matches expected SHA-256 hash.

    Args:
        data: Raw bytes or string to verify
        expected_hash: Expected SHA-256 hash (hex string)

    Returns:
        True if hash matches, False otherwise
    """
    actual = compute_sha256(data)
    return actual == expected_hash.lower()

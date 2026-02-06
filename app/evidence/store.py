# app/evidence/store.py
"""
Immutable evidence store with security.

Evidence is stored by SHA-256 hash of content:
- Content-addressable: same content = same hash = same file
- Immutable: never overwrite existing evidence
- Secure: path traversal prevention via strict hash validation

Directory: ./var/evidence/
Filename: {sha256}.bin
"""

import os
import re
from pathlib import Path
from typing import Optional

from .hashing import compute_sha256

# Evidence storage root - configurable via environment
EVIDENCE_ROOT = Path(os.environ.get("EVIDENCE_ROOT", "./var/evidence/"))


class EvidenceStoreError(Exception):
    """Base exception for evidence store errors."""
    pass


class PathTraversalError(EvidenceStoreError):
    """Raised when path traversal attempt is detected."""
    pass


class InvalidHashError(EvidenceStoreError):
    """Raised when SHA-256 hash format is invalid."""
    pass


def safe_evidence_path(sha256: str) -> Path:
    """
    Generate safe evidence file path.

    SECURITY: Prevents path traversal attacks by:
    1. Validating sha256 is exactly 64 hex characters
    2. Resolving path and verifying it's under EVIDENCE_ROOT

    Args:
        sha256: SHA-256 hash (must be 64 lowercase hex chars)

    Returns:
        Safe Path object under EVIDENCE_ROOT

    Raises:
        InvalidHashError: If sha256 format is invalid
        PathTraversalError: If path traversal attempt detected
    """
    # Validate sha256 is exactly 64 hex chars (lowercase)
    if not re.match(r'^[a-f0-9]{64}$', sha256.lower()):
        raise InvalidHashError(f"Invalid sha256 hash: {sha256}")

    # Normalize to lowercase
    sha256 = sha256.lower()

    # Construct path
    path = EVIDENCE_ROOT / f"{sha256}.bin"

    # Resolve to absolute path and verify it's under EVIDENCE_ROOT
    resolved = path.resolve()
    evidence_root_resolved = EVIDENCE_ROOT.resolve()

    if not str(resolved).startswith(str(evidence_root_resolved)):
        raise PathTraversalError("Path traversal attempt detected")

    return resolved


def store_evidence(raw_bytes: bytes) -> str:
    """
    Store evidence immutably.

    Evidence is stored by SHA-256 hash:
    - If file exists, skip write (content-addressable)
    - If file doesn't exist, create it

    Args:
        raw_bytes: Raw evidence content

    Returns:
        SHA-256 hash of content (use as evidence_id)
    """
    sha256 = compute_sha256(raw_bytes)
    path = safe_evidence_path(sha256)

    # Create directory if needed
    path.parent.mkdir(parents=True, exist_ok=True)

    # Never overwrite - content-addressable means same hash = same content
    if not path.exists():
        path.write_bytes(raw_bytes)

    return sha256


def get_evidence(sha256: str) -> Optional[bytes]:
    """
    Retrieve evidence by SHA-256 hash.

    Args:
        sha256: SHA-256 hash of evidence

    Returns:
        Raw bytes if found, None if not found

    Raises:
        InvalidHashError: If sha256 format is invalid
        PathTraversalError: If path traversal attempt detected
    """
    path = safe_evidence_path(sha256)

    if path.exists():
        return path.read_bytes()
    return None


class EvidenceStore:
    """
    High-level evidence store interface.

    Provides methods for storing, retrieving, and verifying evidence
    with full security guarantees.
    """

    def __init__(self, root: Optional[Path] = None):
        """
        Initialize evidence store.

        Args:
            root: Custom evidence root directory (default: EVIDENCE_ROOT)
        """
        self.root = root or EVIDENCE_ROOT
        self.root.mkdir(parents=True, exist_ok=True)

    def store(self, raw_bytes: bytes) -> str:
        """
        Store evidence and return its SHA-256 hash.

        Args:
            raw_bytes: Raw evidence content

        Returns:
            SHA-256 hash (evidence_id)
        """
        sha256 = compute_sha256(raw_bytes)
        path = self._safe_path(sha256)

        path.parent.mkdir(parents=True, exist_ok=True)

        if not path.exists():
            path.write_bytes(raw_bytes)

        return sha256

    def get(self, sha256: str) -> Optional[bytes]:
        """
        Retrieve evidence by hash.

        Args:
            sha256: SHA-256 hash

        Returns:
            Raw bytes if found, None otherwise
        """
        path = self._safe_path(sha256)
        if path.exists():
            return path.read_bytes()
        return None

    def exists(self, sha256: str) -> bool:
        """
        Check if evidence exists.

        Args:
            sha256: SHA-256 hash

        Returns:
            True if evidence exists
        """
        try:
            path = self._safe_path(sha256)
            return path.exists()
        except (InvalidHashError, PathTraversalError):
            return False

    def verify(self, sha256: str) -> bool:
        """
        Verify evidence integrity.

        Args:
            sha256: SHA-256 hash to verify

        Returns:
            True if evidence exists and hash matches content
        """
        content = self.get(sha256)
        if content is None:
            return False
        return compute_sha256(content) == sha256.lower()

    def _safe_path(self, sha256: str) -> Path:
        """
        Generate safe path with traversal prevention.

        Args:
            sha256: SHA-256 hash

        Returns:
            Safe Path under self.root
        """
        if not re.match(r'^[a-f0-9]{64}$', sha256.lower()):
            raise InvalidHashError(f"Invalid sha256 hash: {sha256}")

        sha256 = sha256.lower()
        path = self.root / f"{sha256}.bin"
        resolved = path.resolve()
        root_resolved = self.root.resolve()

        if not str(resolved).startswith(str(root_resolved)):
            raise PathTraversalError("Path traversal attempt detected")

        return resolved

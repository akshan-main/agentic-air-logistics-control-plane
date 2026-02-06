# tests/test_security.py
"""
Test security features.

Verifies PII redaction and path traversal prevention.
"""

import pytest
from pathlib import Path

from app.evidence.extract import redact_pii, extract_excerpt, PII_PATTERNS
from app.evidence.store import safe_evidence_path, PathTraversalError, InvalidHashError


class TestPIIRedaction:
    """Tests for PII redaction in excerpts."""

    def test_email_redaction(self):
        """Email patterns are redacted."""
        text = "Contact john.doe@example.com for details"
        result = redact_pii(text)

        assert "john.doe@example.com" not in result
        assert "[EMAIL_REDACTED]" in result

    def test_phone_redaction(self):
        """Phone patterns are redacted."""
        test_cases = [
            "Call 555-123-4567 for info",
            "Phone: (555) 123-4567",
            "Contact 555.123.4567",
        ]

        for text in test_cases:
            result = redact_pii(text)
            assert "[PHONE_REDACTED]" in result

    def test_ssn_redaction(self):
        """SSN patterns are redacted."""
        text = "SSN: 123-45-6789"
        result = redact_pii(text)

        assert "123-45-6789" not in result
        assert "[SSN_REDACTED]" in result

    def test_multiple_pii_types(self):
        """Multiple PII types are all redacted."""
        text = "Email: test@example.com, Phone: 555-123-4567, SSN: 123-45-6789"
        result = redact_pii(text)

        assert "[EMAIL_REDACTED]" in result
        assert "[PHONE_REDACTED]" in result
        assert "[SSN_REDACTED]" in result

    def test_non_pii_preserved(self):
        """Non-PII content is preserved."""
        text = "JFK airport has ground stop due to weather. Visibility: 1/4 mile."
        result = redact_pii(text)

        assert result == text  # No changes

    def test_pii_patterns_defined(self):
        """All expected PII patterns are defined."""
        # Check that patterns exist
        assert len(PII_PATTERNS) >= 3  # At least email, phone, SSN

        # Verify patterns are tuples of (pattern, replacement)
        for pattern, replacement in PII_PATTERNS:
            assert isinstance(pattern, str)
            assert isinstance(replacement, str)
            assert "REDACTED" in replacement


class TestExcerptExtraction:
    """Tests for excerpt extraction."""

    def test_extract_excerpt_with_redaction(self):
        """Excerpt extraction includes PII redaction."""
        raw = b"Contact support@example.com for assistance with your order."
        excerpt = extract_excerpt(raw)

        assert "[EMAIL_REDACTED]" in excerpt
        assert "support@example.com" not in excerpt

    def test_excerpt_length_limit(self):
        """Excerpts respect length limit."""
        long_text = "A" * 1000
        raw = long_text.encode('utf-8')

        excerpt = extract_excerpt(raw, max_length=100)
        assert len(excerpt) <= 100

    def test_excerpt_handles_encoding(self):
        """Excerpt handles various encodings gracefully."""
        # Binary data that's not valid UTF-8
        raw = b"\x80\x81\x82 some text"
        excerpt = extract_excerpt(raw)

        assert "some text" in excerpt


class TestPathTraversalPrevention:
    """Tests for path traversal attack prevention."""

    def test_valid_sha256_accepted(self):
        """Valid SHA256 hashes are accepted."""
        valid_hash = "a" * 64
        path = safe_evidence_path(valid_hash)

        assert path.name == f"{valid_hash}.bin"

    def test_invalid_sha256_rejected(self):
        """Invalid SHA256 hashes are rejected."""
        invalid_hashes = [
            "short",  # Too short
            "g" * 64,  # Invalid hex char
            "a" * 63,  # Wrong length
            "a" * 65,  # Wrong length
            "../../../etc/passwd",  # Path traversal attempt
            "a" * 32 + "/../../../etc/passwd",  # Mixed attack
        ]

        for invalid in invalid_hashes:
            with pytest.raises(InvalidHashError):
                safe_evidence_path(invalid)

    def test_path_traversal_blocked(self):
        """Path traversal attempts are blocked."""
        # Even if hash looks valid, traversal should be caught
        # This tests the resolved path check
        traversal_attempts = [
            "../" + "a" * 61,
            "..%2F" + "a" * 58,
        ]

        for attempt in traversal_attempts:
            with pytest.raises((InvalidHashError, PathTraversalError)):
                safe_evidence_path(attempt)

    def test_case_insensitive_hash(self):
        """Hash validation is case-insensitive."""
        upper = "A" * 64
        lower = "a" * 64
        mixed = "aA" * 32

        # All should be accepted
        for h in [upper, lower, mixed]:
            path = safe_evidence_path(h)
            assert path.name.endswith(".bin")


class TestEvidenceStoreImmutability:
    """Tests for evidence store immutability."""

    def test_evidence_file_not_overwritten(self, tmp_path, monkeypatch):
        """Same content doesn't overwrite existing evidence."""
        from app.evidence.store import store_evidence, EVIDENCE_ROOT

        # Patch EVIDENCE_ROOT to use temp directory
        monkeypatch.setattr("app.evidence.store.EVIDENCE_ROOT", tmp_path)

        content = b"test evidence content"

        # Store first time
        hash1 = store_evidence(content)

        # Get file path
        path = tmp_path / f"{hash1}.bin"
        original_mtime = path.stat().st_mtime

        # Store same content again
        hash2 = store_evidence(content)

        # Should return same hash
        assert hash1 == hash2

        # File should not have been modified
        assert path.stat().st_mtime == original_mtime

    def test_different_content_different_hash(self, tmp_path, monkeypatch):
        """Different content produces different hashes."""
        from app.evidence.store import store_evidence

        monkeypatch.setattr("app.evidence.store.EVIDENCE_ROOT", tmp_path)

        hash1 = store_evidence(b"content 1")
        hash2 = store_evidence(b"content 2")

        assert hash1 != hash2

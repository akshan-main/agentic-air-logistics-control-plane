# app/evidence/extract.py
"""
Evidence extraction and PII redaction.

All evidence excerpts are redacted before storage to prevent
PII (Personally Identifiable Information) from being persisted
in the excerpt field of the evidence table.

Redacted patterns:
- Email addresses
- Phone numbers (US format)
- Social Security Numbers (SSN)
"""

import re
from typing import List, Tuple

# PII patterns with their replacements
# Order matters - more specific patterns first
PII_PATTERNS: List[Tuple[str, str]] = [
    # SSN: XXX-XX-XXXX format
    (r'\b\d{3}-\d{2}-\d{4}\b', '[SSN_REDACTED]'),

    # Email addresses
    (r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '[EMAIL_REDACTED]'),

    # Phone numbers: various US formats
    # Matches: 123-456-7890, 123.456.7890, (123) 456-7890, 1234567890
    (r'(?:\(\d{3}\)\s*\d{3}[-.]?\d{4}|\b\d{3}[-.]?\d{3}[-.]?\d{4}\b)', '[PHONE_REDACTED]'),
]


def redact_pii(text: str) -> str:
    """
    Redact PII patterns from text.

    Applied before storing excerpt in evidence table.

    Args:
        text: Input text potentially containing PII

    Returns:
        Text with PII patterns replaced by redaction markers

    Example:
        >>> redact_pii("Contact john@example.com or 555-123-4567")
        'Contact [EMAIL_REDACTED] or [PHONE_REDACTED]'
    """
    result = text
    for pattern, replacement in PII_PATTERNS:
        result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
    return result


def extract_excerpt(raw_payload: bytes, max_length: int = 500) -> str:
    """
    Extract and redact excerpt from evidence payload.

    Converts bytes to text (with error handling for encoding issues),
    truncates to max_length, and applies PII redaction.

    Args:
        raw_payload: Raw evidence bytes
        max_length: Maximum excerpt length (default: 500)

    Returns:
        Redacted excerpt string
    """
    # Decode with replacement for invalid characters
    text = raw_payload.decode('utf-8', errors='replace')

    # Truncate to max length (accounting for "..." suffix)
    if len(text) > max_length:
        text = text[:max_length - 3] + "..."

    # Apply PII redaction
    return redact_pii(text)


def extract_structured_excerpt(
    raw_payload: bytes,
    content_type: str,
    max_length: int = 500
) -> str:
    """
    Extract excerpt based on content type.

    For structured formats (JSON, XML), attempts to extract
    meaningful summary. Falls back to raw excerpt.

    Args:
        raw_payload: Raw evidence bytes
        content_type: MIME type or format identifier
        max_length: Maximum excerpt length

    Returns:
        Redacted excerpt string
    """
    import json

    # Try JSON extraction
    if 'json' in content_type.lower():
        try:
            data = json.loads(raw_payload.decode('utf-8'))
            # Create summary from top-level keys
            if isinstance(data, dict):
                keys = list(data.keys())[:5]
                summary = f"JSON with keys: {', '.join(keys)}"
                if len(data) > 5:
                    summary += f" (+{len(data) - 5} more)"
                return redact_pii(summary)
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

    # Try XML extraction
    if 'xml' in content_type.lower():
        try:
            text = raw_payload.decode('utf-8')
            # Extract root element
            root_match = re.search(r'<(\w+)[>\s]', text)
            if root_match:
                root = root_match.group(1)
                return redact_pii(f"XML document with root: {root}")
        except UnicodeDecodeError:
            pass

    # Default: raw excerpt
    return extract_excerpt(raw_payload, max_length)

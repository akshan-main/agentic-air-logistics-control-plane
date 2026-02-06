# app/logging.py
"""
Structured logging for Agentic Air Logistics Control Plane.

Provides JSON-formatted logging with consistent fields:
- timestamp: ISO 8601
- level: DEBUG/INFO/WARNING/ERROR/CRITICAL
- logger: Module name
- message: Log message
- **kwargs: Additional structured fields

Usage:
    from app.logging import get_logger
    logger = get_logger(__name__)
    logger.info("case_created", case_id=str(case_id), airport="KJFK")
"""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional


class StructuredLogFormatter(logging.Formatter):
    """
    JSON formatter for structured logging.

    Outputs each log line as a JSON object with consistent fields.
    """

    def format(self, record: logging.LogRecord) -> str:
        log_data: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add extra fields from record
        if hasattr(record, "structured_data"):
            log_data.update(record.structured_data)

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Add source location for errors
        if record.levelno >= logging.ERROR:
            log_data["source"] = {
                "file": record.filename,
                "line": record.lineno,
                "function": record.funcName,
            }

        return json.dumps(log_data, default=str)


class StructuredLogger:
    """
    Wrapper around Python logger that supports structured logging.

    Example:
        logger = get_logger(__name__)
        logger.info("evidence_gathered", source="FAA_NAS", count=5)
    """

    def __init__(self, name: str):
        self._logger = logging.getLogger(name)

    def _log(self, level: int, message: str, **kwargs):
        """Internal logging method with structured data."""
        # Create record with extra data
        extra = {"structured_data": kwargs}
        self._logger.log(level, message, extra=extra)

    def debug(self, message: str, **kwargs):
        """Log debug message with structured data."""
        self._log(logging.DEBUG, message, **kwargs)

    def info(self, message: str, **kwargs):
        """Log info message with structured data."""
        self._log(logging.INFO, message, **kwargs)

    def warning(self, message: str, **kwargs):
        """Log warning message with structured data."""
        self._log(logging.WARNING, message, **kwargs)

    def error(self, message: str, exc_info: bool = False, **kwargs):
        """Log error message with structured data."""
        extra = {"structured_data": kwargs}
        self._logger.error(message, exc_info=exc_info, extra=extra)

    def critical(self, message: str, exc_info: bool = False, **kwargs):
        """Log critical message with structured data."""
        extra = {"structured_data": kwargs}
        self._logger.critical(message, exc_info=exc_info, extra=extra)

    def exception(self, message: str, **kwargs):
        """Log exception with structured data (includes traceback)."""
        extra = {"structured_data": kwargs}
        self._logger.exception(message, extra=extra)


# Global configuration state
_configured = False


def configure_logging(
    level: str = "INFO",
    json_output: bool = True,
    log_file: Optional[str] = None,
):
    """
    Configure logging for the application.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        json_output: Use JSON format (True) or plain text (False)
        log_file: Optional file path to write logs to
    """
    global _configured
    if _configured:
        return
    _configured = True

    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))

    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    if json_output:
        console_handler.setFormatter(StructuredLogFormatter())
    else:
        console_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
            )
        )
    root_logger.addHandler(console_handler)

    # File handler (optional)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(StructuredLogFormatter())
        root_logger.addHandler(file_handler)

    # Reduce noise from third-party libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy").setLevel(logging.WARNING)


def get_logger(name: str) -> StructuredLogger:
    """
    Get a structured logger instance.

    Args:
        name: Logger name (typically __name__)

    Returns:
        StructuredLogger instance
    """
    # Auto-configure on first use
    if not _configured:
        configure_logging()
    return StructuredLogger(name)


# Convenience loggers for key components
def get_agent_logger(agent_name: str) -> StructuredLogger:
    """Get logger for an agent component."""
    return get_logger(f"app.agents.{agent_name}")


def get_ingestion_logger(source: str) -> StructuredLogger:
    """Get logger for an ingestion source."""
    return get_logger(f"app.ingestion.{source}")


def get_api_logger() -> StructuredLogger:
    """Get logger for API routes."""
    return get_logger("app.api")

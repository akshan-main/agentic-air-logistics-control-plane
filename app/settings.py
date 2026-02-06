# app/settings.py
"""
Application settings.
"""

import os
from dataclasses import dataclass
from typing import Optional

# Load .env file
from dotenv import load_dotenv
load_dotenv()


@dataclass
class Settings:
    """Application configuration."""

    # Database
    database_url: str = os.getenv(
        "DATABASE_URL",
        "postgresql://forwarder:forwarder@localhost:5432/postgres"
    )

    # Evidence storage
    evidence_root: str = os.getenv("EVIDENCE_ROOT", "./var/evidence/")

    # LLM providers
    openai_api_key: Optional[str] = os.getenv("OPENAI_API_KEY")
    anthropic_api_key: Optional[str] = os.getenv("ANTHROPIC_API_KEY")
    llm_provider: str = os.getenv("LLM_PROVIDER", "openai")
    openai_model: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    anthropic_model: str = os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest")

    # External APIs
    opensky_username: Optional[str] = os.getenv("OPENSKY_USERNAME")
    opensky_password: Optional[str] = os.getenv("OPENSKY_PASSWORD")

    # Ingestion settings
    ingestion_timeout_seconds: int = int(os.getenv("INGESTION_TIMEOUT", "30"))
    opensky_timeout_seconds: int = int(os.getenv("OPENSKY_TIMEOUT", "10"))

    # Agent settings
    max_investigation_budget: int = int(os.getenv("MAX_INVESTIGATION_BUDGET", "20"))
    beam_width: int = int(os.getenv("BEAM_WIDTH", "4"))
    max_beam_depth: int = int(os.getenv("MAX_BEAM_DEPTH", "4"))

    # Replay learning
    playbook_threshold_cases: int = int(os.getenv("PLAYBOOK_THRESHOLD", "3"))

    # API settings
    api_host: str = os.getenv("API_HOST", "0.0.0.0")
    api_port: int = int(os.getenv("API_PORT", "8000"))


# Global settings instance
settings = Settings()

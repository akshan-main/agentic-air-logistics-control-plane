# app/webhooks/registry.py
"""
Webhook registry - manages configured webhook destinations.

Webhooks can be configured for different event types:
- POSTURE_CHANGE: Fired when gateway posture changes (ACCEPT, RESTRICT, HOLD, ESCALATE)
- ACTION_EXECUTED: Fired when an action completes
- CASE_RESOLVED: Fired when a case reaches terminal state
"""

from typing import List, Dict, Optional
from uuid import UUID, uuid4
from datetime import datetime, timezone
from dataclasses import dataclass
from enum import Enum

from sqlalchemy.orm import Session


class WebhookEventType(Enum):
    """Types of events that can trigger webhooks."""
    POSTURE_CHANGE = "POSTURE_CHANGE"
    ACTION_EXECUTED = "ACTION_EXECUTED"
    CASE_RESOLVED = "CASE_RESOLVED"
    SLA_BREACH_IMMINENT = "SLA_BREACH_IMMINENT"


@dataclass
class WebhookConfig:
    """Configuration for a webhook destination."""
    id: UUID
    name: str
    url: str
    event_types: List[WebhookEventType]
    headers: Dict[str, str]
    enabled: bool
    created_at: datetime


# In-memory registry for demo (production would use database)
_WEBHOOK_REGISTRY: Dict[str, WebhookConfig] = {}


class WebhookRegistry:
    """
    Manages webhook configurations.

    In production, this would store configs in database.
    For demo, uses in-memory storage.
    """

    def __init__(self, session: Optional[Session] = None):
        self.session = session

    @classmethod
    def register(
        cls,
        name: str,
        url: str,
        event_types: List[str],
        headers: Optional[Dict[str, str]] = None,
    ) -> WebhookConfig:
        """
        Register a new webhook destination.

        Args:
            name: Human-readable name for the webhook
            url: Destination URL (must be HTTPS in production)
            event_types: List of event types to subscribe to
            headers: Optional custom headers (e.g., Authorization)

        Returns:
            Created webhook config
        """
        webhook_id = uuid4()

        config = WebhookConfig(
            id=webhook_id,
            name=name,
            url=url,
            event_types=[WebhookEventType(et) for et in event_types],
            headers=headers or {},
            enabled=True,
            created_at=datetime.now(timezone.utc),
        )

        _WEBHOOK_REGISTRY[str(webhook_id)] = config
        return config

    @classmethod
    def unregister(cls, webhook_id: str) -> bool:
        """Remove a webhook from registry."""
        if webhook_id in _WEBHOOK_REGISTRY:
            del _WEBHOOK_REGISTRY[webhook_id]
            return True
        return False

    @classmethod
    def get_webhooks_for_event(
        cls,
        event_type: WebhookEventType,
    ) -> List[WebhookConfig]:
        """Get all webhooks subscribed to an event type."""
        return [
            config for config in _WEBHOOK_REGISTRY.values()
            if config.enabled and event_type in config.event_types
        ]

    @classmethod
    def list_all(cls) -> List[WebhookConfig]:
        """List all registered webhooks."""
        return list(_WEBHOOK_REGISTRY.values())

    @classmethod
    def get(cls, webhook_id: str) -> Optional[WebhookConfig]:
        """Get a specific webhook config."""
        return _WEBHOOK_REGISTRY.get(webhook_id)

    @classmethod
    def enable(cls, webhook_id: str) -> bool:
        """Enable a webhook."""
        if webhook_id in _WEBHOOK_REGISTRY:
            # Need to create new instance since dataclass is frozen-ish
            config = _WEBHOOK_REGISTRY[webhook_id]
            _WEBHOOK_REGISTRY[webhook_id] = WebhookConfig(
                id=config.id,
                name=config.name,
                url=config.url,
                event_types=config.event_types,
                headers=config.headers,
                enabled=True,
                created_at=config.created_at,
            )
            return True
        return False

    @classmethod
    def disable(cls, webhook_id: str) -> bool:
        """Disable a webhook (keeps registration but stops firing)."""
        if webhook_id in _WEBHOOK_REGISTRY:
            config = _WEBHOOK_REGISTRY[webhook_id]
            _WEBHOOK_REGISTRY[webhook_id] = WebhookConfig(
                id=config.id,
                name=config.name,
                url=config.url,
                event_types=config.event_types,
                headers=config.headers,
                enabled=False,
                created_at=config.created_at,
            )
            return True
        return False

    @classmethod
    def clear_all(cls):
        """Clear all registered webhooks (for testing)."""
        _WEBHOOK_REGISTRY.clear()

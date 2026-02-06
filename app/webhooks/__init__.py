# app/webhooks/__init__.py
"""
Webhook module for outbound notifications.

Fires HTTP POST requests when posture changes or other events occur.
"""

from .executor import WebhookExecutor
from .registry import WebhookRegistry

__all__ = ["WebhookExecutor", "WebhookRegistry"]

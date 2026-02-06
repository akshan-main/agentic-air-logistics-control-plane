# app/api/routes_webhooks.py
"""
Webhook API routes.

Endpoints for managing webhook destinations and viewing delivery logs.
"""

import ipaddress
import os
from typing import Dict, Any, Optional, List
from datetime import datetime
from dataclasses import asdict
from urllib.parse import urlparse

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, HttpUrl

from ..webhooks import WebhookRegistry, WebhookExecutor
from ..webhooks.registry import WebhookEventType

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


# SSRF protection - block private/internal IP addresses
BLOCKED_IP_RANGES = [
    ipaddress.ip_network("10.0.0.0/8"),       # Private
    ipaddress.ip_network("172.16.0.0/12"),    # Private
    ipaddress.ip_network("192.168.0.0/16"),   # Private
    ipaddress.ip_network("127.0.0.0/8"),      # Loopback
    ipaddress.ip_network("169.254.0.0/16"),   # Link-local
    ipaddress.ip_network("::1/128"),          # IPv6 loopback
    ipaddress.ip_network("fc00::/7"),         # IPv6 unique local
    ipaddress.ip_network("fe80::/10"),        # IPv6 link-local
]

BLOCKED_HOSTNAMES = [
    "localhost",
    "127.0.0.1",
    "0.0.0.0",
    "metadata.google.internal",  # GCP metadata
    "169.254.169.254",           # AWS/Azure metadata
]


def validate_webhook_url(url: str) -> None:
    """
    Validate webhook URL to prevent SSRF attacks.

    Raises HTTPException if URL points to internal/private addresses.
    """
    # Allow bypass in dev mode (set ALLOW_INTERNAL_WEBHOOKS=true)
    if os.getenv("ALLOW_INTERNAL_WEBHOOKS", "").lower() == "true":
        return

    try:
        parsed = urlparse(url)

        # Must be HTTP(S)
        if parsed.scheme not in ("http", "https"):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid URL scheme: {parsed.scheme}. Only http/https allowed."
            )

        hostname = parsed.hostname
        if not hostname:
            raise HTTPException(status_code=400, detail="Invalid URL: no hostname")

        # Check blocked hostnames
        if hostname.lower() in BLOCKED_HOSTNAMES:
            raise HTTPException(
                status_code=400,
                detail=f"Blocked hostname: {hostname}. Internal addresses are not allowed."
            )

        # Try to resolve and check IP
        import socket
        try:
            ip_str = socket.gethostbyname(hostname)
            ip = ipaddress.ip_address(ip_str)

            for blocked_range in BLOCKED_IP_RANGES:
                if ip in blocked_range:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Blocked IP range: {hostname} resolves to {ip_str}. "
                               f"Internal addresses are not allowed for webhooks."
                    )
        except socket.gaierror:
            # Can't resolve - might be a valid external hostname that's just not reachable from here
            pass

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid URL: {str(e)}")


class RegisterWebhookRequest(BaseModel):
    """Request to register a webhook."""
    name: str
    url: str  # Using str instead of HttpUrl for flexibility (allows localhost)
    event_types: List[str]
    headers: Optional[Dict[str, str]] = None


class TestWebhookRequest(BaseModel):
    """Request to test a webhook."""
    url: str
    event_type: str = "POSTURE_CHANGE"


@router.post("/register")
async def register_webhook(request: RegisterWebhookRequest) -> Dict[str, Any]:
    """
    Register a new webhook destination.

    Webhooks receive HTTP POST when events occur:
    - POSTURE_CHANGE: Gateway posture changes (ACCEPT, RESTRICT, HOLD, ESCALATE)
    - ACTION_EXECUTED: An action completes execution
    - CASE_RESOLVED: A case reaches terminal state
    - SLA_BREACH_IMMINENT: SLAs about to breach

    Example payload for POSTURE_CHANGE:
    {
        "event_id": "uuid",
        "event_type": "POSTURE_CHANGE",
        "timestamp": "2024-01-15T10:30:00Z",
        "case_id": "uuid",
        "data": {
            "airport": "KJFK",
            "new_posture": "HOLD",
            "previous_posture": "ACCEPT",
            "confidence": 0.85,
            "evidence_count": 4,
            "risk_level": "HIGH",
            "effective_at": "2024-01-15T10:30:00Z"
        }
    }

    Args:
        request: Webhook configuration

    Returns:
        Created webhook details
    """
    # SSRF protection: validate URL before registration
    validate_webhook_url(request.url)

    # Validate event types
    valid_event_types = {"POSTURE_CHANGE", "ACTION_EXECUTED", "CASE_RESOLVED", "SLA_BREACH_IMMINENT"}
    for et in request.event_types:
        if et not in valid_event_types:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid event type: {et}. Valid types: {valid_event_types}"
            )

    try:
        config = WebhookRegistry.register(
            name=request.name,
            url=request.url,
            event_types=request.event_types,
            headers=request.headers,
        )

        return {
            "webhook_id": str(config.id),
            "name": config.name,
            "url": config.url,
            "event_types": [et.value for et in config.event_types],
            "enabled": config.enabled,
            "created_at": config.created_at.isoformat(),
            "message": "Webhook registered successfully. It will receive HTTP POST for subscribed events.",
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{webhook_id}")
async def unregister_webhook(webhook_id: str) -> Dict[str, Any]:
    """
    Unregister a webhook.

    Args:
        webhook_id: Webhook ID to remove

    Returns:
        Success status
    """
    if WebhookRegistry.unregister(webhook_id):
        return {"deleted": True, "webhook_id": webhook_id}
    else:
        raise HTTPException(status_code=404, detail=f"Webhook not found: {webhook_id}")


@router.get("")
async def list_webhooks() -> Dict[str, Any]:
    """
    List all registered webhooks.

    Returns:
        List of webhook configurations
    """
    webhooks = WebhookRegistry.list_all()

    return {
        "webhooks": [
            {
                "webhook_id": str(w.id),
                "name": w.name,
                "url": w.url,
                "event_types": [et.value for et in w.event_types],
                "enabled": w.enabled,
                "created_at": w.created_at.isoformat(),
            }
            for w in webhooks
        ],
        "count": len(webhooks),
        "supported_event_types": [
            {
                "type": "POSTURE_CHANGE",
                "description": "Fired when gateway posture changes (ACCEPT, RESTRICT, HOLD, ESCALATE)",
            },
            {
                "type": "ACTION_EXECUTED",
                "description": "Fired when an action completes execution",
            },
            {
                "type": "CASE_RESOLVED",
                "description": "Fired when a case reaches terminal state",
            },
            {
                "type": "SLA_BREACH_IMMINENT",
                "description": "Fired when SLAs are about to breach",
            },
        ],
    }


@router.post("/{webhook_id}/enable")
async def enable_webhook(webhook_id: str) -> Dict[str, Any]:
    """
    Enable a disabled webhook.

    Args:
        webhook_id: Webhook ID

    Returns:
        Updated status
    """
    if WebhookRegistry.enable(webhook_id):
        return {"webhook_id": webhook_id, "enabled": True}
    else:
        raise HTTPException(status_code=404, detail=f"Webhook not found: {webhook_id}")


@router.post("/{webhook_id}/disable")
async def disable_webhook(webhook_id: str) -> Dict[str, Any]:
    """
    Disable a webhook (stops firing but keeps registration).

    Args:
        webhook_id: Webhook ID

    Returns:
        Updated status
    """
    if WebhookRegistry.disable(webhook_id):
        return {"webhook_id": webhook_id, "enabled": False}
    else:
        raise HTTPException(status_code=404, detail=f"Webhook not found: {webhook_id}")


@router.get("/deliveries")
async def get_deliveries(limit: int = 50) -> Dict[str, Any]:
    """
    Get recent webhook delivery log.

    Shows history of webhook deliveries including success/failure status.

    Args:
        limit: Maximum number of deliveries to return

    Returns:
        Delivery log
    """
    deliveries = WebhookExecutor.get_delivery_log(limit=limit)

    return {
        "deliveries": [
            {
                "delivery_id": str(d.delivery_id),
                "webhook_id": str(d.webhook_id),
                "webhook_name": d.webhook_name,
                "url": d.url,
                "event_type": d.event_type,
                "success": d.success,
                "response_status": d.response_status,
                "error": d.error,
                "delivered_at": d.delivered_at.isoformat(),
            }
            for d in deliveries
        ],
        "count": len(deliveries),
    }


@router.post("/test")
async def test_webhook(request: TestWebhookRequest) -> Dict[str, Any]:
    """
    Test a webhook destination with a sample payload.

    Sends a test event to verify the webhook is reachable.
    Does NOT require registration.

    Args:
        request: Test webhook config

    Returns:
        Test delivery result
    """
    # SSRF protection: validate URL before testing
    validate_webhook_url(request.url)

    from uuid import uuid4
    from ..webhooks.executor import WebhookPayload, WebhookConfig

    # Create temporary webhook config for testing
    test_config = WebhookConfig(
        id=uuid4(),
        name="Test Webhook",
        url=request.url,
        event_types=[WebhookEventType(request.event_type)],
        headers={},
        enabled=True,
        created_at=datetime.utcnow(),
    )

    # Create test payload
    test_payload = WebhookPayload(
        event_id=str(uuid4()),
        event_type=request.event_type,
        timestamp=datetime.utcnow().isoformat(),
        case_id="test-case-123",
        data={
            "airport": "KJFK",
            "new_posture": "RESTRICT",
            "previous_posture": "ACCEPT",
            "confidence": 0.75,
            "evidence_count": 3,
            "risk_level": "MEDIUM",
            "effective_at": datetime.utcnow().isoformat(),
            "_test": True,
        }
    )

    # Send test
    with WebhookExecutor() as executor:
        delivery = executor._deliver(test_config, test_payload)

    return {
        "success": delivery.success,
        "url": request.url,
        "response_status": delivery.response_status,
        "response_body": delivery.response_body,
        "error": delivery.error,
        "payload_sent": asdict(test_payload),
        "message": "Test webhook delivered successfully" if delivery.success else f"Test failed: {delivery.error}",
    }


@router.delete("/deliveries")
async def clear_deliveries() -> Dict[str, Any]:
    """
    Clear the delivery log.

    Returns:
        Success status
    """
    WebhookExecutor.clear_delivery_log()
    return {"cleared": True, "message": "Delivery log cleared"}

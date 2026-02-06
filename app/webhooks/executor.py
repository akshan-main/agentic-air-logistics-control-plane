# app/webhooks/executor.py
"""
Webhook executor - fires HTTP POST to registered destinations.

This is the "system that DOES something" when posture changes.
"""

import json
import httpx
from typing import Dict, Any, List, Optional
from uuid import UUID, uuid4
from datetime import datetime, timezone
from dataclasses import dataclass, asdict

from ..logging import get_logger
from .registry import WebhookRegistry, WebhookEventType, WebhookConfig

logger = get_logger(__name__)


@dataclass
class WebhookPayload:
    """Standard webhook payload structure."""
    event_id: str
    event_type: str
    timestamp: str
    case_id: Optional[str]
    data: Dict[str, Any]


@dataclass
class WebhookDelivery:
    """Record of a webhook delivery attempt."""
    delivery_id: UUID
    webhook_id: UUID
    webhook_name: str
    url: str
    event_type: str
    payload: Dict[str, Any]
    response_status: Optional[int]
    response_body: Optional[str]
    success: bool
    error: Optional[str]
    delivered_at: datetime


# In-memory delivery log for demo
_DELIVERY_LOG: List[WebhookDelivery] = []


class WebhookExecutor:
    """
    Executes webhooks when events occur.

    Fires HTTP POST requests to all registered destinations
    for a given event type.
    """

    def __init__(self, timeout_seconds: float = 10.0):
        self.timeout = timeout_seconds
        self.client = httpx.Client(timeout=timeout_seconds)

    def fire_posture_change(
        self,
        case_id: str,
        airport: str,
        new_posture: str,
        previous_posture: Optional[str] = None,
        confidence: Optional[float] = None,
        evidence_count: Optional[int] = None,
        risk_level: Optional[str] = None,
    ) -> List[WebhookDelivery]:
        """
        Fire POSTURE_CHANGE webhooks.

        This is called when SET_POSTURE action executes.

        Args:
            case_id: Case that triggered the change
            airport: Airport ICAO code
            new_posture: New posture (ACCEPT, RESTRICT, HOLD, ESCALATE)
            previous_posture: Previous posture if known
            confidence: Decision confidence 0-1
            evidence_count: Number of evidence items
            risk_level: Risk level (LOW, MEDIUM, HIGH, CRITICAL)

        Returns:
            List of delivery records
        """
        payload = WebhookPayload(
            event_id=str(uuid4()),
            event_type="POSTURE_CHANGE",
            timestamp=datetime.now(timezone.utc).isoformat(),
            case_id=case_id,
            data={
                "airport": airport,
                "new_posture": new_posture,
                "previous_posture": previous_posture,
                "confidence": confidence,
                "evidence_count": evidence_count,
                "risk_level": risk_level,
                "effective_at": datetime.now(timezone.utc).isoformat(),
            }
        )

        return self._fire_event(WebhookEventType.POSTURE_CHANGE, payload)

    def fire_action_executed(
        self,
        case_id: str,
        action_id: str,
        action_type: str,
        success: bool,
        result: Dict[str, Any],
    ) -> List[WebhookDelivery]:
        """
        Fire ACTION_EXECUTED webhooks.

        Args:
            case_id: Case ID
            action_id: Executed action ID
            action_type: Action type (SET_POSTURE, HOLD_CARGO, etc.)
            success: Whether action succeeded
            result: Action result data

        Returns:
            List of delivery records
        """
        payload = WebhookPayload(
            event_id=str(uuid4()),
            event_type="ACTION_EXECUTED",
            timestamp=datetime.now(timezone.utc).isoformat(),
            case_id=case_id,
            data={
                "action_id": action_id,
                "action_type": action_type,
                "success": success,
                "result": result,
            }
        )

        return self._fire_event(WebhookEventType.ACTION_EXECUTED, payload)

    def fire_case_resolved(
        self,
        case_id: str,
        final_posture: str,
        status: str,
        metrics: Dict[str, Any],
    ) -> List[WebhookDelivery]:
        """
        Fire CASE_RESOLVED webhooks.

        Args:
            case_id: Case ID
            final_posture: Final posture decision
            status: Final case status (RESOLVED, BLOCKED)
            metrics: Case metrics (PDL, evidence count, etc.)

        Returns:
            List of delivery records
        """
        payload = WebhookPayload(
            event_id=str(uuid4()),
            event_type="CASE_RESOLVED",
            timestamp=datetime.now(timezone.utc).isoformat(),
            case_id=case_id,
            data={
                "final_posture": final_posture,
                "status": status,
                "metrics": metrics,
            }
        )

        return self._fire_event(WebhookEventType.CASE_RESOLVED, payload)

    def fire_sla_breach_imminent(
        self,
        case_id: str,
        airport: str,
        shipments_at_risk: List[Dict[str, Any]],
        total_value_at_risk: float,
    ) -> List[WebhookDelivery]:
        """
        Fire SLA_BREACH_IMMINENT webhooks.

        Args:
            case_id: Case ID
            airport: Airport ICAO code
            shipments_at_risk: List of shipments with imminent SLA breach
            total_value_at_risk: Total revenue at risk

        Returns:
            List of delivery records
        """
        payload = WebhookPayload(
            event_id=str(uuid4()),
            event_type="SLA_BREACH_IMMINENT",
            timestamp=datetime.now(timezone.utc).isoformat(),
            case_id=case_id,
            data={
                "airport": airport,
                "shipments_at_risk_count": len(shipments_at_risk),
                "shipments_at_risk": shipments_at_risk[:10],  # Limit payload size
                "total_value_at_risk_usd": total_value_at_risk,
            }
        )

        return self._fire_event(WebhookEventType.SLA_BREACH_IMMINENT, payload)

    def _fire_event(
        self,
        event_type: WebhookEventType,
        payload: WebhookPayload,
    ) -> List[WebhookDelivery]:
        """
        Fire webhooks for an event type.

        Args:
            event_type: Type of event
            payload: Payload to send

        Returns:
            List of delivery records
        """
        webhooks = WebhookRegistry.get_webhooks_for_event(event_type)
        deliveries = []

        logger.info(
            "firing_webhooks",
            event_type=event_type.value,
            webhook_count=len(webhooks),
            event_id=payload.event_id,
        )

        for webhook in webhooks:
            delivery = self._deliver(webhook, payload)
            deliveries.append(delivery)
            _DELIVERY_LOG.append(delivery)

        return deliveries

    def _deliver(
        self,
        webhook: WebhookConfig,
        payload: WebhookPayload,
    ) -> WebhookDelivery:
        """
        Deliver payload to a single webhook destination.

        Args:
            webhook: Webhook config
            payload: Payload to send

        Returns:
            Delivery record
        """
        delivery_id = uuid4()
        payload_dict = asdict(payload)

        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Forwarder-Exception-OS/1.0",
            "X-Webhook-Event": payload.event_type,
            "X-Webhook-Delivery-ID": str(delivery_id),
            **webhook.headers,  # Custom headers (e.g., Authorization)
        }

        try:
            logger.info(
                "webhook_delivery_attempt",
                webhook_id=str(webhook.id),
                webhook_name=webhook.name,
                url=webhook.url,
                event_type=payload.event_type,
            )

            response = self.client.post(
                webhook.url,
                json=payload_dict,
                headers=headers,
            )

            success = 200 <= response.status_code < 300

            logger.info(
                "webhook_delivery_complete",
                webhook_id=str(webhook.id),
                webhook_name=webhook.name,
                status_code=response.status_code,
                success=success,
            )

            return WebhookDelivery(
                delivery_id=delivery_id,
                webhook_id=webhook.id,
                webhook_name=webhook.name,
                url=webhook.url,
                event_type=payload.event_type,
                payload=payload_dict,
                response_status=response.status_code,
                response_body=response.text[:500] if response.text else None,
                success=success,
                error=None if success else f"HTTP {response.status_code}",
                delivered_at=datetime.now(timezone.utc),
            )

        except httpx.TimeoutException as e:
            logger.warning(
                "webhook_delivery_timeout",
                webhook_id=str(webhook.id),
                webhook_name=webhook.name,
                url=webhook.url,
                error=str(e),
            )

            return WebhookDelivery(
                delivery_id=delivery_id,
                webhook_id=webhook.id,
                webhook_name=webhook.name,
                url=webhook.url,
                event_type=payload.event_type,
                payload=payload_dict,
                response_status=None,
                response_body=None,
                success=False,
                error=f"Timeout: {str(e)}",
                delivered_at=datetime.now(timezone.utc),
            )

        except Exception as e:
            logger.error(
                "webhook_delivery_failed",
                webhook_id=str(webhook.id),
                webhook_name=webhook.name,
                url=webhook.url,
                error=str(e),
            )

            return WebhookDelivery(
                delivery_id=delivery_id,
                webhook_id=webhook.id,
                webhook_name=webhook.name,
                url=webhook.url,
                event_type=payload.event_type,
                payload=payload_dict,
                response_status=None,
                response_body=None,
                success=False,
                error=str(e),
                delivered_at=datetime.now(timezone.utc),
            )

    @staticmethod
    def get_delivery_log(limit: int = 100) -> List[WebhookDelivery]:
        """Get recent delivery log."""
        return list(reversed(_DELIVERY_LOG[-limit:]))

    @staticmethod
    def clear_delivery_log():
        """Clear delivery log (for testing)."""
        _DELIVERY_LOG.clear()

    def close(self):
        """Close HTTP client."""
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

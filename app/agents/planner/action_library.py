# app/agents/planner/action_library.py
"""
Action library - defines available actions and their properties.

Actions are categorized by:
- Shipment-level: Require booking evidence
- Posture-level: No booking required
- Operational: System-to-system, no booking required
"""

from typing import Dict, Any, Set


# Shipment-level actions (require booking evidence)
SHIPMENT_ACTIONS: Set[str] = {
    "HOLD_CARGO",
    "RELEASE_CARGO",
    "SWITCH_GATEWAY",
    "REBOOK_FLIGHT",
    "UPGRADE_SERVICE",
    "NOTIFY_CUSTOMER",
    "FILE_CLAIM",
}

# Posture-level actions (no booking required)
POSTURE_ACTIONS: Set[str] = {
    "SET_POSTURE",
}

# Operational actions (system-to-system, no booking required)
OPERATIONAL_ACTIONS: Set[str] = {
    "PUBLISH_GATEWAY_ADVISORY",
    "UPDATE_BOOKING_RULES",
    "TRIGGER_REEVALUATION",
    "ESCALATE_OPS",
}

# All action types
ACTION_TYPES: Set[str] = SHIPMENT_ACTIONS | POSTURE_ACTIONS | OPERATIONAL_ACTIONS


# Action definitions with properties
ACTION_DEFINITIONS: Dict[str, Dict[str, Any]] = {
    # Shipment actions
    "HOLD_CARGO": {
        "description": "Place cargo on hold pending review",
        "risk_level": "MEDIUM",
        "requires_approval": False,
        "requires_booking": True,
        "reversible": True,
        "notification_required": True,
    },
    "RELEASE_CARGO": {
        "description": "Release held cargo for processing",
        "risk_level": "LOW",
        "requires_approval": False,
        "requires_booking": True,
        "reversible": False,
        "notification_required": False,
    },
    "SWITCH_GATEWAY": {
        "description": "Reroute cargo to alternative gateway",
        "risk_level": "HIGH",
        "requires_approval": True,
        "requires_booking": True,
        "reversible": False,
        "notification_required": True,
    },
    "REBOOK_FLIGHT": {
        "description": "Rebook cargo on different flight",
        "risk_level": "HIGH",
        "requires_approval": True,
        "requires_booking": True,
        "reversible": False,
        "notification_required": True,
    },
    "UPGRADE_SERVICE": {
        "description": "Upgrade service level to meet deadline",
        "risk_level": "MEDIUM",
        "requires_approval": True,
        "requires_booking": True,
        "reversible": False,
        "notification_required": True,
    },
    "NOTIFY_CUSTOMER": {
        "description": "Send notification to customer",
        "risk_level": "MEDIUM",
        "requires_approval": False,
        "requires_booking": True,
        "reversible": False,
        "notification_required": True,
    },
    "FILE_CLAIM": {
        "description": "File claim for damages or delays",
        "risk_level": "HIGH",
        "requires_approval": True,
        "requires_booking": True,
        "reversible": False,
        "notification_required": True,
    },

    # Posture actions
    "SET_POSTURE": {
        "description": "Set gateway posture directive",
        "risk_level": "LOW",
        "requires_approval": False,
        "requires_booking": False,
        "reversible": True,
        "notification_required": False,
    },

    # Operational actions
    "PUBLISH_GATEWAY_ADVISORY": {
        "description": "Publish advisory to downstream systems",
        "risk_level": "LOW",
        "requires_approval": False,
        "requires_booking": False,
        "reversible": True,
        "notification_required": False,
    },
    "UPDATE_BOOKING_RULES": {
        "description": "Update rules engine for new bookings",
        "risk_level": "MEDIUM",
        "requires_approval": False,
        "requires_booking": False,
        "reversible": True,
        "notification_required": False,
    },
    "TRIGGER_REEVALUATION": {
        "description": "Force re-evaluation of pending decisions",
        "risk_level": "LOW",
        "requires_approval": False,
        "requires_booking": False,
        "reversible": True,
        "notification_required": False,
    },
    "ESCALATE_OPS": {
        "description": "Escalate to duty manager",
        "risk_level": "LOW",
        "requires_approval": False,
        "requires_booking": False,
        "reversible": False,
        "notification_required": True,
    },
}


def get_action_risk_level(action_type: str) -> str:
    """Get risk level for action type."""
    definition = ACTION_DEFINITIONS.get(action_type, {})
    return definition.get("risk_level", "MEDIUM")


def get_action_definition(action_type: str) -> Dict[str, Any]:
    """Get full definition for action type."""
    return ACTION_DEFINITIONS.get(action_type, {})


def requires_booking_evidence(action_type: str) -> bool:
    """Check if action requires booking evidence."""
    return action_type in SHIPMENT_ACTIONS


def requires_approval(action_type: str) -> bool:
    """Check if action requires approval by default."""
    definition = ACTION_DEFINITIONS.get(action_type, {})
    return definition.get("requires_approval", False)


def requires_notification(action_type: str) -> bool:
    """Check if action requires notification."""
    definition = ACTION_DEFINITIONS.get(action_type, {})
    return definition.get("notification_required", False)

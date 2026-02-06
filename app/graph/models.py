# app/graph/models.py
"""
Graph models for the bi-temporal context graph.

Core entities:
- Node: Immutable graph node (type + identifier)
- NodeVersion: Versioned attributes for a node
- Edge: Bi-temporal edge with status (DRAFT/FACT/RETRACTED)
"""

from datetime import datetime
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field
from uuid import UUID


@dataclass
class Node:
    """
    Immutable graph node.

    Nodes cannot be updated after creation.
    Use NodeVersion for attribute changes.
    """
    id: UUID
    type: str  # AIRPORT, FLIGHT, CARRIER, etc.
    identifier: str  # Human-readable (e.g., "KJFK")
    created_at: datetime


@dataclass
class NodeVersion:
    """
    Versioned attributes for a node.

    Each attribute change creates a new version.
    Uses supersedes_id for audit trail.
    """
    id: UUID
    node_id: UUID
    attrs: Dict[str, Any]
    valid_from: datetime
    valid_to: Optional[datetime]  # NULL = current
    supersedes_id: Optional[UUID]  # Previous version
    created_at: datetime


@dataclass
class Edge:
    """
    Bi-temporal edge with evidence binding.

    Edges can be DRAFT (no evidence required) or FACT (evidence required).
    Uses supersedes_edge_id for audit trail.
    """
    id: UUID
    src: UUID  # Source node ID
    dst: UUID  # Destination node ID
    type: str  # AIRPORT_HAS_FAA_DISRUPTION, etc.
    attrs: Dict[str, Any]
    # Status for evidence binding
    status: str  # DRAFT, FACT, RETRACTED
    supersedes_edge_id: Optional[UUID]
    # Bi-temporal
    event_time_start: Optional[datetime]
    event_time_end: Optional[datetime]
    ingested_at: datetime
    # Validity window
    valid_from: Optional[datetime]
    valid_to: Optional[datetime]
    # Provenance
    source_system: str
    confidence: float


@dataclass
class Claim:
    """
    Factual claim with evidence binding.

    Claims must have status DRAFT, HYPOTHESIS, FACT, or RETRACTED.
    FACT claims require evidence binding (enforced by DB trigger).
    """
    id: UUID
    text: str
    subject_node_id: Optional[UUID]
    confidence: float
    status: str  # DRAFT, HYPOTHESIS, FACT, RETRACTED
    supersedes_claim_id: Optional[UUID]
    event_time_start: Optional[datetime]
    event_time_end: Optional[datetime]
    ingested_at: datetime


@dataclass
class GraphSubset:
    """
    Subset of graph returned by traversal.

    Contains nodes, edges, and optional claims
    reachable from starting nodes.
    """
    nodes: List[Node] = field(default_factory=list)
    node_versions: List[NodeVersion] = field(default_factory=list)
    edges: List[Edge] = field(default_factory=list)
    claims: List[Claim] = field(default_factory=list)

    @property
    def node_count(self) -> int:
        return len(self.nodes)

    @property
    def edge_count(self) -> int:
        return len(self.edges)

    def get_node_by_id(self, node_id: UUID) -> Optional[Node]:
        """Find node by ID."""
        for node in self.nodes:
            if node.id == node_id:
                return node
        return None

    def get_edges_from(self, node_id: UUID) -> List[Edge]:
        """Get all edges starting from a node."""
        return [e for e in self.edges if e.src == node_id]

    def get_edges_to(self, node_id: UUID) -> List[Edge]:
        """Get all edges ending at a node."""
        return [e for e in self.edges if e.dst == node_id]

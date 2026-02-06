# app/graph/store.py
"""
Graph store for persisting and querying the context graph.

All temporal queries use CANONICAL visibility predicates from visibility.py.
"""

import json
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..db.engine import session_scope, SessionLocal
from .models import Node, NodeVersion, Edge, Claim, GraphSubset
from .visibility import edge_visible_at, node_version_visible_at, get_visibility_params


class GraphStore:
    """
    Persistent store for the bi-temporal context graph.

    All methods use canonical visibility predicates.
    """

    def __init__(self, session: Optional[Session] = None):
        """
        Initialize graph store.

        Args:
            session: Optional SQLAlchemy session (creates new if not provided)
        """
        self._session = session
        self._owns_session = session is None

    @property
    def session(self) -> Session:
        """Get or create session."""
        if self._session is None:
            self._session = SessionLocal()
        return self._session

    def close(self):
        """Close session if we own it."""
        if self._owns_session and self._session is not None:
            self._session.close()
            self._session = None

    # ============================================================
    # NODE OPERATIONS
    # ============================================================

    def create_node(
        self,
        type: str,
        identifier: str,
        attrs: Optional[Dict[str, Any]] = None,
    ) -> Node:
        """
        Create a new node with initial version.

        Args:
            type: Node type (AIRPORT, FLIGHT, etc.)
            identifier: Human-readable identifier
            attrs: Initial attributes

        Returns:
            Created Node

        Note:
            Nodes are immutable after creation.
            Use create_node_version for attribute changes.
        """
        node_id = uuid4()
        now = datetime.now(timezone.utc)

        # Insert node
        self.session.execute(
            text("""
                INSERT INTO node (id, type, identifier, created_at)
                VALUES (:id, :type, :identifier, :created_at)
            """),
            {"id": node_id, "type": type, "identifier": identifier, "created_at": now}
        )

        # Insert initial version
        if attrs:
            self.session.execute(
                text("""
                    INSERT INTO node_version (id, node_id, attrs, valid_from, created_at)
                    VALUES (:id, :node_id, CAST(:attrs AS jsonb), :valid_from, :created_at)
                """),
                {
                    "id": uuid4(),
                    "node_id": node_id,
                    "attrs": json.dumps(attrs),
                    "valid_from": now,
                    "created_at": now,
                }
            )

        self.session.commit()

        return Node(id=node_id, type=type, identifier=identifier, created_at=now)

    def get_node(self, node_id: UUID) -> Optional[Node]:
        """Get node by ID."""
        result = self.session.execute(
            text("SELECT id, type, identifier, created_at FROM node WHERE id = :id"),
            {"id": node_id}
        )
        row = result.fetchone()
        if row:
            return Node(id=row[0], type=row[1], identifier=row[2], created_at=row[3])
        return None

    def get_node_by_identifier(self, type: str, identifier: str) -> Optional[Node]:
        """Get node by type and identifier."""
        result = self.session.execute(
            text("""
                SELECT id, type, identifier, created_at FROM node
                WHERE type = :type AND identifier = :identifier
            """),
            {"type": type, "identifier": identifier}
        )
        row = result.fetchone()
        if row:
            return Node(id=row[0], type=row[1], identifier=row[2], created_at=row[3])
        return None

    def create_node_version(
        self,
        node_id: UUID,
        attrs: Dict[str, Any],
        supersedes_id: Optional[UUID] = None,
    ) -> NodeVersion:
        """
        Create new version of a node's attributes.

        Args:
            node_id: Node to version
            attrs: New attributes
            supersedes_id: Previous version being replaced

        Returns:
            Created NodeVersion
        """
        version_id = uuid4()
        now = datetime.now(timezone.utc)

        # Close previous version if specified
        if supersedes_id:
            self.session.execute(
                text("""
                    UPDATE node_version SET valid_to = :now
                    WHERE id = :id AND valid_to IS NULL
                """),
                {"id": supersedes_id, "now": now}
            )

        # Insert new version
        self.session.execute(
            text("""
                INSERT INTO node_version
                (id, node_id, attrs, valid_from, supersedes_id, created_at)
                VALUES (:id, :node_id, CAST(:attrs AS jsonb), :valid_from, :supersedes_id, :created_at)
            """),
            {
                "id": version_id,
                "node_id": node_id,
                "attrs": json.dumps(attrs),
                "valid_from": now,
                "supersedes_id": supersedes_id,
                "created_at": now,
            }
        )

        self.session.commit()

        return NodeVersion(
            id=version_id,
            node_id=node_id,
            attrs=attrs,
            valid_from=now,
            valid_to=None,
            supersedes_id=supersedes_id,
            created_at=now,
        )

    # ============================================================
    # EDGE OPERATIONS
    # ============================================================

    def create_edge(
        self,
        src: UUID,
        dst: UUID,
        type: str,
        source_system: str,
        attrs: Optional[Dict[str, Any]] = None,
        status: str = "DRAFT",
        confidence: float = 0.5,
        event_time_start: Optional[datetime] = None,
        event_time_end: Optional[datetime] = None,
        valid_from: Optional[datetime] = None,
        valid_to: Optional[datetime] = None,
    ) -> Edge:
        """
        Create a new edge.

        Args:
            src: Source node ID
            dst: Destination node ID
            type: Edge type
            source_system: Data source
            attrs: Edge attributes
            status: DRAFT, FACT, or RETRACTED
            confidence: Confidence score
            event_time_start: When this edge became true in reality
            event_time_end: When this edge stopped being true
            valid_from: Validity window start
            valid_to: Validity window end

        Returns:
            Created Edge

        Note:
            Create as DRAFT, then add evidence, then UPDATE to FACT.
        """
        edge_id = uuid4()
        now = datetime.now(timezone.utc)

        self.session.execute(
            text("""
                INSERT INTO edge
                (id, src, dst, type, attrs, status, event_time_start, event_time_end,
                 ingested_at, valid_from, valid_to, source_system, confidence)
                VALUES
                (:id, :src, :dst, :type, CAST(:attrs AS jsonb), :status, :event_time_start, :event_time_end,
                 :ingested_at, :valid_from, :valid_to, :source_system, :confidence)
            """),
            {
                "id": edge_id,
                "src": src,
                "dst": dst,
                "type": type,
                "attrs": json.dumps(attrs or {}),
                "status": status,
                "event_time_start": event_time_start,
                "event_time_end": event_time_end,
                "ingested_at": now,
                "valid_from": valid_from,
                "valid_to": valid_to,
                "source_system": source_system,
                "confidence": confidence,
            }
        )

        self.session.commit()

        return Edge(
            id=edge_id,
            src=src,
            dst=dst,
            type=type,
            attrs=attrs or {},
            status=status,
            supersedes_edge_id=None,
            event_time_start=event_time_start,
            event_time_end=event_time_end,
            ingested_at=now,
            valid_from=valid_from,
            valid_to=valid_to,
            source_system=source_system,
            confidence=confidence,
        )

    def promote_edge_to_fact(self, edge_id: UUID) -> bool:
        """
        Promote edge from DRAFT to FACT.

        Args:
            edge_id: Edge to promote

        Returns:
            True if successful

        Raises:
            Exception: If edge has no evidence (trigger will fail)
        """
        self.session.execute(
            text("UPDATE edge SET status = 'FACT' WHERE id = :id"),
            {"id": edge_id}
        )
        self.session.commit()
        return True

    def get_edges_visible_at(
        self,
        at_event_time: datetime,
        at_ingest_time: datetime,
        edge_types: Optional[List[str]] = None,
        src_node_id: Optional[UUID] = None,
    ) -> List[Edge]:
        """
        Get edges visible at a point in time.

        Uses CANONICAL visibility predicate.

        Args:
            at_event_time: Event time point
            at_ingest_time: Ingest time point
            edge_types: Optional filter by edge types
            src_node_id: Optional filter by source node

        Returns:
            List of visible edges
        """
        visibility = edge_visible_at(at_event_time, at_ingest_time)
        params = get_visibility_params(at_event_time, at_ingest_time)

        where_clauses = [visibility]
        if edge_types:
            where_clauses.append("type = ANY(:edge_types)")
            params["edge_types"] = edge_types
        if src_node_id:
            where_clauses.append("src = :src_node_id")
            params["src_node_id"] = src_node_id

        query = f"""
            SELECT id, src, dst, type, attrs, status, supersedes_edge_id,
                   event_time_start, event_time_end, ingested_at,
                   valid_from, valid_to, source_system, confidence
            FROM edge
            WHERE {' AND '.join(where_clauses)}
        """

        result = self.session.execute(text(query), params)
        edges = []
        for row in result:
            edges.append(Edge(
                id=row[0], src=row[1], dst=row[2], type=row[3],
                attrs=row[4], status=row[5], supersedes_edge_id=row[6],
                event_time_start=row[7], event_time_end=row[8],
                ingested_at=row[9], valid_from=row[10], valid_to=row[11],
                source_system=row[12], confidence=row[13],
            ))
        return edges

    # ============================================================
    # CLAIM OPERATIONS
    # ============================================================

    def create_claim(
        self,
        text_content: str,
        confidence: float,
        status: str = "DRAFT",
        subject_node_id: Optional[UUID] = None,
        event_time_start: Optional[datetime] = None,
        event_time_end: Optional[datetime] = None,
    ) -> Claim:
        """
        Create a new claim.

        Args:
            text_content: Claim text
            confidence: Confidence score
            status: DRAFT, HYPOTHESIS, FACT, or RETRACTED
            subject_node_id: Node this claim is about
            event_time_start: When claim became true
            event_time_end: When claim stopped being true

        Returns:
            Created Claim

        Note:
            Create as DRAFT, add claim_evidence, then UPDATE to FACT.
        """
        claim_id = uuid4()
        now = datetime.now(timezone.utc)

        self.session.execute(
            text("""
                INSERT INTO claim
                (id, text, subject_node_id, confidence, status,
                 event_time_start, event_time_end, ingested_at)
                VALUES
                (:id, :text, :subject_node_id, :confidence, :status,
                 :event_time_start, :event_time_end, :ingested_at)
            """),
            {
                "id": claim_id,
                "text": text_content,
                "subject_node_id": subject_node_id,
                "confidence": confidence,
                "status": status,
                "event_time_start": event_time_start,
                "event_time_end": event_time_end,
                "ingested_at": now,
            }
        )

        self.session.commit()

        return Claim(
            id=claim_id,
            text=text_content,
            subject_node_id=subject_node_id,
            confidence=confidence,
            status=status,
            supersedes_claim_id=None,
            event_time_start=event_time_start,
            event_time_end=event_time_end,
            ingested_at=now,
        )

    def add_claim_evidence(self, claim_id: UUID, evidence_id: UUID):
        """Add evidence binding to claim."""
        self.session.execute(
            text("""
                INSERT INTO claim_evidence (claim_id, evidence_id)
                VALUES (:claim_id, :evidence_id)
                ON CONFLICT DO NOTHING
            """),
            {"claim_id": claim_id, "evidence_id": evidence_id}
        )
        self.session.commit()

    def add_edge_evidence(self, edge_id: UUID, evidence_id: UUID):
        """Add evidence binding to edge."""
        self.session.execute(
            text("""
                INSERT INTO edge_evidence (edge_id, evidence_id)
                VALUES (:edge_id, :evidence_id)
                ON CONFLICT DO NOTHING
            """),
            {"edge_id": edge_id, "evidence_id": evidence_id}
        )
        self.session.commit()

    def promote_claim_to_fact(self, claim_id: UUID) -> bool:
        """
        Promote claim from DRAFT to FACT.

        Args:
            claim_id: Claim to promote

        Returns:
            True if successful

        Raises:
            Exception: If claim has no evidence (trigger will fail)
        """
        self.session.execute(
            text("UPDATE claim SET status = 'FACT' WHERE id = :id"),
            {"id": claim_id}
        )
        self.session.commit()
        return True


# Singleton instance
_graph_store: Optional[GraphStore] = None


def get_graph_store() -> GraphStore:
    """Get or create singleton GraphStore."""
    global _graph_store
    if _graph_store is None:
        _graph_store = GraphStore()
    return _graph_store

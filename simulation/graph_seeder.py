# simulation/graph_seeder.py
"""
Seeds the context graph with simulated operational data.

Creates graph nodes and edges for:
- CARRIER nodes
- FLIGHT nodes (connected to airports)
- SHIPMENT nodes (connected to flights)
- BOOKING nodes (evidence for shipment actions)
- DOCUMENT nodes (customs, AWB, etc.)

Edge types:
- CARRIER_OPERATES_FLIGHT: Carrier -> Flight
- FLIGHT_DEPARTS_FROM: Flight -> Airport
- FLIGHT_ARRIVES_AT: Flight -> Airport
- SHIPMENT_ON_FLIGHT: Shipment -> Flight
- SHIPMENT_ORIGIN: Shipment -> Airport
- SHIPMENT_DESTINATION: Shipment -> Airport
- BOOKING_FOR_SHIPMENT: Booking -> Shipment
- BOOKING_WITH_CARRIER: Booking -> Carrier
- DOCUMENT_FOR_SHIPMENT: Document -> Shipment
"""

import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.graph.store import GraphStore, get_graph_store
from app.graph.models import Node
from app.evidence.store import store_evidence, EVIDENCE_ROOT
from app.evidence.extract import extract_excerpt
from .operational_data import (
    OperationalDataGenerator,
    default_operational_seed_for_airport,
    SimulatedCarrier,
    SimulatedFlight,
    SimulatedShipment,
    SimulatedBooking,
    SimulatedDocument,
)

def _execute_values(
    session: Session,
    sql: str,
    rows: List[Tuple[Any, ...]],
    template: str,
    page_size: int = 1000,
) -> None:
    """
    Execute a VALUES bulk insert efficiently using psycopg2's execute_values.

    This runs on the Session's underlying DBAPI connection so it participates
    in the current transaction.
    """
    if not rows:
        return

    try:
        from psycopg2.extras import execute_values  # type: ignore[import-not-found]
    except Exception as e:  # pragma: no cover - psycopg2-binary is a hard dependency
        raise RuntimeError("psycopg2 is required for bulk SIMULATION seeding") from e

    connection = session.connection()
    dbapi_connection = connection.connection

    with dbapi_connection.cursor() as cursor:
        execute_values(cursor, sql, rows, template=template, page_size=page_size)


class GraphSeeder:
    """
    Seeds the context graph with operational data.

    This makes the graph useful for:
    1. Cascade analysis: airport disruption -> flights -> shipments
    2. Shipment-level actions with booking evidence
    3. Document deadline tracking
    """

    def __init__(self, graph_store: Optional[GraphStore] = None, seed: Optional[int] = None):
        self.graph = graph_store or get_graph_store()
        self.seed = seed

        # Track created node IDs for edge creation
        self.node_ids: Dict[str, UUID] = {}

    def seed_airport(
        self,
        airport_icao: str,
        num_flights: int = 20,
        shipments_per_flight: int = 5,
        *,
        bulk: bool = True,
        commit: bool = True,
    ) -> Dict[str, Any]:
        """
        Seed graph with operational data for an airport.

        Args:
            airport_icao: Airport ICAO code
            num_flights: Number of flights to generate
            shipments_per_flight: Shipments per flight

        Returns:
            Summary of seeded data
        """
        if bulk:
            return self._seed_airport_bulk(
                airport_icao=airport_icao,
                num_flights=num_flights,
                shipments_per_flight=shipments_per_flight,
                commit=commit,
            )

        # Fallback row-by-row implementation (debug-friendly, slower).
        airport_icao = airport_icao.upper()
        seeded_at = datetime.now(timezone.utc)

        seed_used = self.seed if self.seed is not None else default_operational_seed_for_airport(airport_icao)
        generator = OperationalDataGenerator(seed=seed_used)
        data = generator.generate_full_dataset_for_airport(
            airport_icao,
            num_flights=num_flights,
            shipments_per_flight=shipments_per_flight,
        )

        try:
            # Ensure airport node exists
            self._ensure_airport_node(airport_icao)

            # Create carrier nodes
            carrier_nodes = self._create_carrier_nodes(data["carriers"])

            # Create flight nodes and edges
            flight_nodes = self._create_flight_nodes_and_edges(
                data["flights"],
                carrier_nodes,
                seeded_at=seeded_at,
            )

            # Create shipment nodes and edges
            shipment_nodes = self._create_shipment_nodes_and_edges(
                data["shipments"],
                flight_nodes,
            )

            # Create booking nodes and edges (CRITICAL for shipment actions)
            booking_nodes = self._create_booking_nodes_and_edges(
                data["bookings"],
                shipment_nodes,
                carrier_nodes,
            )

            # Create document nodes and edges
            document_nodes = self._create_document_nodes_and_edges(
                data["documents"],
                shipment_nodes,
            )

            if commit:
                self.graph.session.commit()

            return {
                "airport": airport_icao,
                "seed_used": seed_used,
                "nodes_created": {
                    "carriers": len(carrier_nodes),
                    "flights": len(flight_nodes),
                    "shipments": len(shipment_nodes),
                    "bookings": len(booking_nodes),
                    "documents": len(document_nodes),
                },
                "stats": data["stats"],
            }
        except Exception:
            self.graph.session.rollback()
            raise

    def _seed_airport_bulk(
        self,
        airport_icao: str,
        num_flights: int,
        shipments_per_flight: int,
        *,
        commit: bool,
    ) -> Dict[str, Any]:
        """
        Fast SIMULATION seeding path.

        Uses a single transaction + bulk inserts to avoid thousands of roundtrips
        (critical for remote Postgres like Supabase).
        """
        airport_icao = airport_icao.upper()
        seeded_at = datetime.now(timezone.utc)

        seed_used = self.seed if self.seed is not None else default_operational_seed_for_airport(airport_icao)
        generator = OperationalDataGenerator(seed=seed_used)
        data = generator.generate_full_dataset_for_airport(
            airport_icao,
            num_flights=num_flights,
            shipments_per_flight=shipments_per_flight,
        )

        flights: List[SimulatedFlight] = data["flights"]
        shipments: List[SimulatedShipment] = data["shipments"]
        bookings: List[SimulatedBooking] = data["bookings"]
        documents: List[SimulatedDocument] = data["documents"]
        carriers: List[SimulatedCarrier] = data["carriers"]

        carrier_by_id: Dict[str, SimulatedCarrier] = {c.id: c for c in carriers}

        # Airports referenced by the operational dataset (flights + shipments)
        airport_icaos: set[str] = {airport_icao}
        airport_icaos.update({f.origin_icao.upper() for f in flights})
        airport_icaos.update({f.destination_icao.upper() for f in flights})
        airport_icaos.update({s.origin_icao.upper() for s in shipments})
        airport_icaos.update({s.destination_icao.upper() for s in shipments})

        carrier_ids: set[str] = {c.id for c in carriers}

        session = self.graph.session

        try:
            # Resolve existing AIRPORT/CARRIER node IDs (avoid unique conflicts).
            airports_existing = session.execute(
                text("""
                    SELECT id, identifier
                    FROM node
                    WHERE type = 'AIRPORT'
                      AND identifier = ANY(:ids)
                """),
                {"ids": sorted(airport_icaos)},
            ).fetchall()
            airport_node_ids: Dict[str, UUID] = {row[1]: row[0] for row in airports_existing}

            carriers_existing = session.execute(
                text("""
                    SELECT id, identifier
                    FROM node
                    WHERE type = 'CARRIER'
                      AND identifier = ANY(:ids)
                """),
                {"ids": sorted(carrier_ids)},
            ).fetchall()
            carrier_node_ids: Dict[str, UUID] = {row[1]: row[0] for row in carriers_existing}

            node_rows: List[Tuple[Any, ...]] = []
            node_version_rows: List[Tuple[Any, ...]] = []

            # Create missing airport nodes
            missing_airports = [icao for icao in sorted(airport_icaos) if icao not in airport_node_ids]
            for icao in missing_airports:
                node_id = uuid4()
                airport_node_ids[icao] = node_id
                node_rows.append((node_id, "AIRPORT", icao, seeded_at))
                node_version_rows.append((uuid4(), node_id, json.dumps({"icao": icao}), seeded_at, seeded_at))

            # Create missing carrier nodes
            missing_carriers = [cid for cid in sorted(carrier_ids) if cid not in carrier_node_ids]
            for cid in missing_carriers:
                carrier = carrier_by_id[cid]
                node_id = uuid4()
                carrier_node_ids[cid] = node_id
                node_rows.append((node_id, "CARRIER", cid, seeded_at))
                node_version_rows.append((
                    uuid4(),
                    node_id,
                    json.dumps(
                        {
                            "name": carrier.name,
                            "iata_code": carrier.iata_code,
                            "hub_airports": carrier.hub_airports,
                        }
                    ),
                    seeded_at,
                    seeded_at,
                ))

            # Create FLIGHT nodes
            flight_node_ids: Dict[str, UUID] = {}
            for flight in flights:
                node_id = uuid4()
                flight_node_ids[flight.id] = node_id
                node_rows.append((node_id, "FLIGHT", flight.id, seeded_at))
                node_version_rows.append((
                    uuid4(),
                    node_id,
                    json.dumps(
                        {
                            "flight_number": flight.flight_number,
                            "carrier_id": flight.carrier_id,
                            "origin": flight.origin_icao,
                            "destination": flight.destination_icao,
                            "scheduled_departure": flight.scheduled_departure.isoformat(),
                            "scheduled_arrival": flight.scheduled_arrival.isoformat(),
                            "status": flight.status,
                            "aircraft_type": flight.aircraft_type,
                        }
                    ),
                    seeded_at,
                    seeded_at,
                ))

            # Create SHIPMENT nodes
            shipment_node_ids: Dict[str, UUID] = {}
            for shipment in shipments:
                node_id = uuid4()
                shipment_node_ids[shipment.id] = node_id
                node_rows.append((node_id, "SHIPMENT", shipment.id, seeded_at))
                node_version_rows.append((
                    uuid4(),
                    node_id,
                    json.dumps(
                        {
                            "tracking_number": shipment.tracking_number,
                            "flight_id": shipment.flight_id,
                            "origin": shipment.origin_icao,
                            "destination": shipment.destination_icao,
                            "weight_kg": shipment.weight_kg,
                            "pieces": shipment.pieces,
                            "commodity": shipment.commodity,
                            "shipper": shipment.shipper,
                            "consignee": shipment.consignee,
                            "service_level": shipment.service_level,
                            "status": shipment.status,
                        }
                    ),
                    seeded_at,
                    seeded_at,
                ))

            # Create BOOKING nodes + evidence rows
            booking_node_ids: Dict[str, UUID] = {}
            evidence_rows: List[Tuple[Any, ...]] = []

            for booking in bookings:
                booking_attrs = {
                    "booking_reference": booking.booking_reference,
                    "shipment_id": booking.shipment_id,
                    "flight_id": booking.flight_id,
                    "carrier_id": booking.carrier_id,
                    "booked_at": booking.booked_at.isoformat(),
                    "customer_id": booking.customer_id,
                    "sla_deadline": booking.sla_deadline.isoformat(),
                    "rate_type": booking.rate_type,
                    "rate_per_kg": booking.rate_per_kg,
                    "total_charge_usd": booking.total_charge_usd,
                    "margin_percent": booking.margin_percent,
                }

                node_id = uuid4()
                booking_node_ids[booking.id] = node_id
                node_rows.append((node_id, "BOOKING", booking.id, seeded_at))
                node_version_rows.append((uuid4(), node_id, json.dumps(booking_attrs), seeded_at, seeded_at))

                raw_bytes = json.dumps(
                    {"source": "BOOKING", "booking_id": booking.id, **booking_attrs},
                    default=str,
                ).encode("utf-8")
                sha256 = store_evidence(raw_bytes)
                excerpt = extract_excerpt(raw_bytes)
                source_ref = f"booking:{booking.id}"

                evidence_rows.append((
                    uuid4(),
                    "BOOKING",
                    source_ref,
                    booking.booked_at,
                    "application/json",
                    sha256,
                    str(EVIDENCE_ROOT / f"{sha256}.bin"),
                    excerpt,
                    json.dumps(
                        {
                            "booking_reference": booking.booking_reference,
                            "shipment_id": booking.shipment_id,
                        }
                    ),
                ))

            # Create DOCUMENT nodes
            document_node_ids: Dict[str, UUID] = {}
            for doc in documents:
                node_id = uuid4()
                document_node_ids[doc.id] = node_id
                node_rows.append((node_id, "DOCUMENT", doc.id, seeded_at))

                attrs: Dict[str, Any] = {
                    "document_type": doc.document_type,
                    "document_number": doc.document_number,
                    "shipment_id": doc.shipment_id,
                    "issued_at": doc.issued_at.isoformat(),
                    "status": doc.status,
                }
                if doc.deadline:
                    attrs["deadline"] = doc.deadline.isoformat()

                node_version_rows.append((uuid4(), node_id, json.dumps(attrs), seeded_at, seeded_at))

            # Bulk insert nodes + node_versions
            if node_rows:
                _execute_values(
                    session,
                    "INSERT INTO node (id, type, identifier, created_at) VALUES %s",
                    node_rows,
                    template="(%s, %s, %s, %s)",
                    page_size=1000,
                )
            if node_version_rows:
                _execute_values(
                    session,
                    "INSERT INTO node_version (id, node_id, attrs, valid_from, created_at) VALUES %s",
                    node_version_rows,
                    template="(%s, %s, %s::jsonb, %s, %s)",
                    page_size=1000,
                )

            # Build edges (UUIDs are generated client-side for speed)
            edge_rows: List[Tuple[Any, ...]] = []

            for flight in flights:
                flight_node_id = flight_node_ids[flight.id]
                origin_id = airport_node_ids[flight.origin_icao.upper()]
                dest_id = airport_node_ids[flight.destination_icao.upper()]

                # FLIGHT_DEPARTS_FROM / FLIGHT_ARRIVES_AT: schedule relationship known at seed time
                edge_rows.append((
                    uuid4(),
                    flight_node_id,
                    origin_id,
                    "FLIGHT_DEPARTS_FROM",
                    json.dumps({"scheduled_departure": flight.scheduled_departure.isoformat()}),
                    "DRAFT",
                    seeded_at,
                    None,
                    seeded_at,
                    None,
                    None,
                    "SIMULATION",
                    1.0,
                ))
                edge_rows.append((
                    uuid4(),
                    flight_node_id,
                    dest_id,
                    "FLIGHT_ARRIVES_AT",
                    json.dumps({"scheduled_arrival": flight.scheduled_arrival.isoformat()}),
                    "DRAFT",
                    seeded_at,
                    None,
                    seeded_at,
                    None,
                    None,
                    "SIMULATION",
                    1.0,
                ))

                carrier_id = carrier_node_ids.get(flight.carrier_id)
                if carrier_id:
                    edge_rows.append((
                        uuid4(),
                        carrier_id,
                        flight_node_id,
                        "CARRIER_OPERATES_FLIGHT",
                        json.dumps({"flight_number": flight.flight_number}),
                        "DRAFT",
                        None,
                        None,
                        seeded_at,
                        None,
                        None,
                        "SIMULATION",
                        1.0,
                    ))

            for shipment in shipments:
                shipment_node_id = shipment_node_ids[shipment.id]

                flight_node_id = flight_node_ids.get(shipment.flight_id)
                if flight_node_id:
                    edge_rows.append((
                        uuid4(),
                        shipment_node_id,
                        flight_node_id,
                        "SHIPMENT_ON_FLIGHT",
                        json.dumps({"weight_kg": shipment.weight_kg, "pieces": shipment.pieces}),
                        "DRAFT",
                        None,
                        None,
                        seeded_at,
                        None,
                        None,
                        "SIMULATION",
                        1.0,
                    ))

                origin_id = airport_node_ids[shipment.origin_icao.upper()]
                dest_id = airport_node_ids[shipment.destination_icao.upper()]
                edge_rows.append((
                    uuid4(),
                    shipment_node_id,
                    origin_id,
                    "SHIPMENT_ORIGIN",
                    json.dumps({}),
                    "DRAFT",
                    None,
                    None,
                    seeded_at,
                    None,
                    None,
                    "SIMULATION",
                    1.0,
                ))
                edge_rows.append((
                    uuid4(),
                    shipment_node_id,
                    dest_id,
                    "SHIPMENT_DESTINATION",
                    json.dumps({}),
                    "DRAFT",
                    None,
                    None,
                    seeded_at,
                    None,
                    None,
                    "SIMULATION",
                    1.0,
                ))

            for booking in bookings:
                booking_node_id = booking_node_ids[booking.id]

                shipment_node_id = shipment_node_ids.get(booking.shipment_id)
                if shipment_node_id:
                    edge_rows.append((
                        uuid4(),
                        booking_node_id,
                        shipment_node_id,
                        "BOOKING_FOR_SHIPMENT",
                        json.dumps(
                            {
                                "total_charge_usd": booking.total_charge_usd,
                                "sla_deadline": booking.sla_deadline.isoformat(),
                            }
                        ),
                        "DRAFT",
                        None,
                        None,
                        seeded_at,
                        None,
                        None,
                        "SIMULATION",
                        1.0,
                    ))

                carrier_node_id = carrier_node_ids.get(booking.carrier_id)
                if carrier_node_id:
                    edge_rows.append((
                        uuid4(),
                        booking_node_id,
                        carrier_node_id,
                        "BOOKING_WITH_CARRIER",
                        json.dumps({"rate_type": booking.rate_type}),
                        "DRAFT",
                        None,
                        None,
                        seeded_at,
                        None,
                        None,
                        "SIMULATION",
                        1.0,
                    ))

            for doc in documents:
                document_node_id = document_node_ids[doc.id]
                shipment_node_id = shipment_node_ids.get(doc.shipment_id)
                if shipment_node_id:
                    edge_rows.append((
                        uuid4(),
                        document_node_id,
                        shipment_node_id,
                        "DOCUMENT_FOR_SHIPMENT",
                        json.dumps({"document_type": doc.document_type, "status": doc.status}),
                        "DRAFT",
                        None,
                        None,
                        seeded_at,
                        None,
                        None,
                        "SIMULATION",
                        1.0,
                    ))

            if edge_rows:
                _execute_values(
                    session,
                    """
                    INSERT INTO edge
                    (id, src, dst, type, attrs, status, event_time_start, event_time_end,
                     ingested_at, valid_from, valid_to, source_system, confidence)
                    VALUES %s
                    """.strip(),
                    edge_rows,
                    template="(%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s)",
                    page_size=1000,
                )

            # Bulk insert booking evidence rows (PolicyJudge requires source_system='BOOKING').
            if evidence_rows:
                try:
                    _execute_values(
                        session,
                        """
                        INSERT INTO evidence
                        (id, source_system, source_ref, retrieved_at, content_type,
                         payload_sha256, raw_path, excerpt, meta)
                        VALUES %s
                        ON CONFLICT (source_system, source_ref, payload_sha256) DO NOTHING
                        """.strip(),
                        evidence_rows,
                        template="(%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)",
                        page_size=1000,
                    )
                except Exception as e:
                    msg = str(e)
                    if "no unique or exclusion constraint matching the ON CONFLICT specification" in msg:
                        raise RuntimeError(
                            "Database schema is missing the evidence dedup constraint required for booking evidence upserts. "
                            "Apply migrations: `./setup.sh` (idempotent) or `make migrate`."
                        ) from e
                    raise

            if commit:
                session.commit()

            return {
                "airport": airport_icao,
                "seed_used": seed_used,
                "nodes_created": {
                    "carriers": len(carriers),
                    "flights": len(flights),
                    "shipments": len(shipments),
                    "bookings": len(bookings),
                    "documents": len(documents),
                },
                "stats": data["stats"],
            }
        except Exception:
            session.rollback()
            raise

    def _ensure_airport_node(self, icao: str) -> Node:
        """Get or create airport node."""
        existing = self.graph.get_node_by_identifier("AIRPORT", icao)
        if existing:
            self.node_ids[f"AIRPORT:{icao}"] = existing.id
            return existing

        node = self.graph.create_node(
            type="AIRPORT",
            identifier=icao,
            attrs={"icao": icao}
        )
        self.node_ids[f"AIRPORT:{icao}"] = node.id
        return node

    def _create_carrier_nodes(self, carriers: List[SimulatedCarrier]) -> Dict[str, Node]:
        """Create carrier nodes."""
        nodes = {}
        for carrier in carriers:
            key = f"CARRIER:{carrier.id}"

            # Check if already exists
            existing = self.graph.get_node_by_identifier("CARRIER", carrier.id)
            if existing:
                nodes[carrier.id] = existing
                self.node_ids[key] = existing.id
                continue

            node = self.graph.create_node(
                type="CARRIER",
                identifier=carrier.id,
                attrs={
                    "name": carrier.name,
                    "iata_code": carrier.iata_code,
                    "hub_airports": carrier.hub_airports,
                }
            )
            nodes[carrier.id] = node
            self.node_ids[key] = node.id

        return nodes

    def _create_flight_nodes_and_edges(
        self,
        flights: List[SimulatedFlight],
        carrier_nodes: Dict[str, Node],
        seeded_at: datetime,
    ) -> Dict[str, Node]:
        """Create flight nodes and edges to airports/carriers."""
        nodes = {}

        for flight in flights:
            key = f"FLIGHT:{flight.id}"

            # Check if already exists
            existing = self.graph.get_node_by_identifier("FLIGHT", flight.id)
            if existing:
                nodes[flight.id] = existing
                self.node_ids[key] = existing.id
                continue

            # Create flight node
            node = self.graph.create_node(
                type="FLIGHT",
                identifier=flight.id,
                attrs={
                    "flight_number": flight.flight_number,
                    "carrier_id": flight.carrier_id,
                    "origin": flight.origin_icao,
                    "destination": flight.destination_icao,
                    "scheduled_departure": flight.scheduled_departure.isoformat(),
                    "scheduled_arrival": flight.scheduled_arrival.isoformat(),
                    "status": flight.status,
                    "aircraft_type": flight.aircraft_type,
                }
            )
            nodes[flight.id] = node
            self.node_ids[key] = node.id

            # Ensure origin/destination airports exist
            origin_node = self._ensure_airport_node(flight.origin_icao)
            dest_node = self._ensure_airport_node(flight.destination_icao)

            # Edge: FLIGHT_DEPARTS_FROM (Flight -> Airport)
            self.graph.create_edge(
                src=node.id,
                dst=origin_node.id,
                type="FLIGHT_DEPARTS_FROM",
                source_system="SIMULATION",
                attrs={
                    "scheduled_departure": flight.scheduled_departure.isoformat(),
                },
                status="DRAFT",  # Simulation data doesn't need evidence
                confidence=1.0,
                # This is a schedule relationship (known at seed time), not the actual departure event.
                # Keeping event_time_start <= now ensures operational graph is visible for near-future planning.
                event_time_start=seeded_at,
            )

            # Edge: FLIGHT_ARRIVES_AT (Flight -> Airport)
            self.graph.create_edge(
                src=node.id,
                dst=dest_node.id,
                type="FLIGHT_ARRIVES_AT",
                source_system="SIMULATION",
                attrs={
                    "scheduled_arrival": flight.scheduled_arrival.isoformat(),
                },
                status="DRAFT",
                confidence=1.0,
                event_time_start=seeded_at,
            )

            # Edge: CARRIER_OPERATES_FLIGHT (Carrier -> Flight)
            if flight.carrier_id in carrier_nodes:
                carrier_node = carrier_nodes[flight.carrier_id]
                self.graph.create_edge(
                    src=carrier_node.id,
                    dst=node.id,
                    type="CARRIER_OPERATES_FLIGHT",
                    source_system="SIMULATION",
                    attrs={
                        "flight_number": flight.flight_number,
                    },
                    status="DRAFT",
                    confidence=1.0,
                )

        return nodes

    def _create_shipment_nodes_and_edges(
        self,
        shipments: List[SimulatedShipment],
        flight_nodes: Dict[str, Node]
    ) -> Dict[str, Node]:
        """Create shipment nodes and edges to flights/airports."""
        nodes = {}

        for shipment in shipments:
            key = f"SHIPMENT:{shipment.id}"

            # Check if already exists
            existing = self.graph.get_node_by_identifier("SHIPMENT", shipment.id)
            if existing:
                nodes[shipment.id] = existing
                self.node_ids[key] = existing.id
                continue

            # Create shipment node
            node = self.graph.create_node(
                type="SHIPMENT",
                identifier=shipment.id,
                attrs={
                    "tracking_number": shipment.tracking_number,
                    "flight_id": shipment.flight_id,
                    "origin": shipment.origin_icao,
                    "destination": shipment.destination_icao,
                    "weight_kg": shipment.weight_kg,
                    "pieces": shipment.pieces,
                    "commodity": shipment.commodity,
                    "shipper": shipment.shipper,
                    "consignee": shipment.consignee,
                    "service_level": shipment.service_level,
                    "status": shipment.status,
                }
            )
            nodes[shipment.id] = node
            self.node_ids[key] = node.id

            # Edge: SHIPMENT_ON_FLIGHT (Shipment -> Flight)
            if shipment.flight_id in flight_nodes:
                flight_node = flight_nodes[shipment.flight_id]
                self.graph.create_edge(
                    src=node.id,
                    dst=flight_node.id,
                    type="SHIPMENT_ON_FLIGHT",
                    source_system="SIMULATION",
                    attrs={
                        "weight_kg": shipment.weight_kg,
                        "pieces": shipment.pieces,
                    },
                    status="DRAFT",
                    confidence=1.0,
                )

            # Edge: SHIPMENT_ORIGIN (Shipment -> Airport)
            origin_node = self._ensure_airport_node(shipment.origin_icao)
            self.graph.create_edge(
                src=node.id,
                dst=origin_node.id,
                type="SHIPMENT_ORIGIN",
                source_system="SIMULATION",
                attrs={},
                status="DRAFT",
                confidence=1.0,
            )

            # Edge: SHIPMENT_DESTINATION (Shipment -> Airport)
            dest_node = self._ensure_airport_node(shipment.destination_icao)
            self.graph.create_edge(
                src=node.id,
                dst=dest_node.id,
                type="SHIPMENT_DESTINATION",
                source_system="SIMULATION",
                attrs={},
                status="DRAFT",
                confidence=1.0,
            )

        return nodes

    def _create_booking_nodes_and_edges(
        self,
        bookings: List[SimulatedBooking],
        shipment_nodes: Dict[str, Node],
        carrier_nodes: Dict[str, Node]
    ) -> Dict[str, Node]:
        """
        Create booking nodes, edges, AND evidence rows.

        CRITICAL: The PolicyJudge checks evidence.source_system='BOOKING'
        to allow shipment-level actions. Without evidence rows here,
        _has_booking_evidence() always returns False and shipment actions
        are permanently blocked.
        """
        nodes = {}

        for booking in bookings:
            key = f"BOOKING:{booking.id}"

            # Check if already exists
            existing = self.graph.get_node_by_identifier("BOOKING", booking.id)
            if existing:
                nodes[booking.id] = existing
                self.node_ids[key] = existing.id
                continue

            booking_attrs = {
                "booking_reference": booking.booking_reference,
                "shipment_id": booking.shipment_id,
                "flight_id": booking.flight_id,
                "carrier_id": booking.carrier_id,
                "booked_at": booking.booked_at.isoformat(),
                "customer_id": booking.customer_id,
                "sla_deadline": booking.sla_deadline.isoformat(),
                "rate_type": booking.rate_type,
                "rate_per_kg": booking.rate_per_kg,
                "total_charge_usd": booking.total_charge_usd,
                "margin_percent": booking.margin_percent,
            }

            # Create booking node
            node = self.graph.create_node(
                type="BOOKING",
                identifier=booking.id,
                attrs=booking_attrs,
            )
            nodes[booking.id] = node
            self.node_ids[key] = node.id

            # Create evidence row so PolicyJudge._has_booking_evidence() finds it.
            # source_system='BOOKING' is what the guardrail queries for.
            self._create_booking_evidence(booking, booking_attrs)

            # Edge: BOOKING_FOR_SHIPMENT (Booking -> Shipment)
            if booking.shipment_id in shipment_nodes:
                shipment_node = shipment_nodes[booking.shipment_id]
                self.graph.create_edge(
                    src=node.id,
                    dst=shipment_node.id,
                    type="BOOKING_FOR_SHIPMENT",
                    source_system="SIMULATION",
                    attrs={
                        "total_charge_usd": booking.total_charge_usd,
                        "sla_deadline": booking.sla_deadline.isoformat(),
                    },
                    status="DRAFT",
                    confidence=1.0,
                )

            # Edge: BOOKING_WITH_CARRIER (Booking -> Carrier)
            if booking.carrier_id in carrier_nodes:
                carrier_node = carrier_nodes[booking.carrier_id]
                self.graph.create_edge(
                    src=node.id,
                    dst=carrier_node.id,
                    type="BOOKING_WITH_CARRIER",
                    source_system="SIMULATION",
                    attrs={
                        "rate_type": booking.rate_type,
                    },
                    status="DRAFT",
                    confidence=1.0,
                )

        return nodes

    def _create_booking_evidence(
        self,
        booking: SimulatedBooking,
        booking_attrs: Dict[str, Any],
    ) -> None:
        """
        Create an evidence table row for a booking.

        This bridges the gap between graph nodes (BOOKING type) and the
        evidence table (source_system='BOOKING') that PolicyJudge queries.
        """
        raw_bytes = json.dumps({
            "source": "BOOKING",
            "booking_id": booking.id,
            **booking_attrs,
        }, default=str).encode("utf-8")

        sha256 = store_evidence(raw_bytes)
        excerpt = extract_excerpt(raw_bytes)

        # Determine airport from shipment origin (bookings are linked to shipments)
        # Use the shipment's origin airport for the source_ref
        source_ref = f"booking:{booking.id}"

        try:
            self.graph.session.execute(
                text("""
                    INSERT INTO evidence
                    (id, source_system, source_ref, retrieved_at, content_type,
                     payload_sha256, raw_path, excerpt, meta)
                    VALUES
                    (:id, 'BOOKING', :source_ref, :retrieved_at, 'application/json',
                     :payload_sha256, :raw_path, :excerpt, CAST(:meta AS jsonb))
                    ON CONFLICT (source_system, source_ref, payload_sha256) DO NOTHING
                """),
                {
                    "id": uuid4(),
                    "source_ref": source_ref,
                    "retrieved_at": booking.booked_at,
                    "payload_sha256": sha256,
                    "raw_path": str(EVIDENCE_ROOT / f"{sha256}.bin"),
                    "excerpt": excerpt,
                    "meta": json.dumps({
                        "booking_reference": booking.booking_reference,
                        "shipment_id": booking.shipment_id,
                    }),
                },
            )
        except Exception as e:
            # If this fails, shipment-level actions will be blocked (PolicyJudge requires BOOKING evidence).
            self.graph.session.rollback()
            msg = str(e)
            if "no unique or exclusion constraint matching the ON CONFLICT specification" in msg:
                raise RuntimeError(
                    "Database schema is missing the evidence dedup constraint required for booking evidence upserts. "
                    "Apply migrations: `./setup.sh` (idempotent) or `make migrate`."
                ) from e
            raise

    def _create_document_nodes_and_edges(
        self,
        documents: List[SimulatedDocument],
        shipment_nodes: Dict[str, Node]
    ) -> Dict[str, Node]:
        """Create document nodes and edges to shipments."""
        nodes = {}

        for doc in documents:
            key = f"DOCUMENT:{doc.id}"

            # Check if already exists
            existing = self.graph.get_node_by_identifier("DOCUMENT", doc.id)
            if existing:
                nodes[doc.id] = existing
                self.node_ids[key] = existing.id
                continue

            # Create document node
            attrs = {
                "document_type": doc.document_type,
                "document_number": doc.document_number,
                "shipment_id": doc.shipment_id,
                "issued_at": doc.issued_at.isoformat(),
                "status": doc.status,
            }
            if doc.deadline:
                attrs["deadline"] = doc.deadline.isoformat()

            node = self.graph.create_node(
                type="DOCUMENT",
                identifier=doc.id,
                attrs=attrs
            )
            nodes[doc.id] = node
            self.node_ids[key] = node.id

            # Edge: DOCUMENT_FOR_SHIPMENT (Document -> Shipment)
            if doc.shipment_id in shipment_nodes:
                shipment_node = shipment_nodes[doc.shipment_id]
                self.graph.create_edge(
                    src=node.id,
                    dst=shipment_node.id,
                    type="DOCUMENT_FOR_SHIPMENT",
                    source_system="SIMULATION",
                    attrs={
                        "document_type": doc.document_type,
                        "status": doc.status,
                    },
                    status="DRAFT",
                    confidence=1.0,
                )

        return nodes


OPERATIONAL_EDGE_TYPES = (
    "CARRIER_OPERATES_FLIGHT",
    "FLIGHT_DEPARTS_FROM",
    "FLIGHT_ARRIVES_AT",
    "SHIPMENT_ON_FLIGHT",
    "SHIPMENT_ORIGIN",
    "SHIPMENT_DESTINATION",
    "BOOKING_FOR_SHIPMENT",
    "BOOKING_WITH_CARRIER",
    "DOCUMENT_FOR_SHIPMENT",
)

OPERATIONAL_NODE_TYPES = (
    "CARRIER",
    "FLIGHT",
    "SHIPMENT",
    "BOOKING",
    "DOCUMENT",
)


def clear_seeded_operational_data_for_airport(
    airport_icao: str,
    session: Optional[Session] = None,
    *,
    commit: bool = True,
) -> Dict[str, Any]:
    """
    Clear SIMULATION-seeded operational graph data for a single airport.

    This deletes only edges with source_system='SIMULATION' plus now-orphaned
    operational nodes (CARRIER/FLIGHT/SHIPMENT/BOOKING/DOCUMENT). AIRPORT nodes
    are never deleted.
    """
    from app.db.engine import SessionLocal

    airport_icao = airport_icao.upper()

    owns_session = session is None
    if session is None:
        session = SessionLocal()

    try:
        # Flights connected to this airport (SIMULATION edges only)
        flights_result = session.execute(
            text("""
                SELECT DISTINCT e.src
                FROM edge e
                JOIN node a ON e.dst = a.id
                WHERE a.type = 'AIRPORT'
                  AND a.identifier = :icao
                  AND e.type IN ('FLIGHT_DEPARTS_FROM', 'FLIGHT_ARRIVES_AT')
                  AND e.source_system = 'SIMULATION'
            """),
            {"icao": airport_icao},
        )
        flight_node_ids = [row[0] for row in flights_result]

        shipment_node_ids: List[UUID] = []
        booking_node_ids: List[UUID] = []
        document_node_ids: List[UUID] = []
        carrier_node_ids: List[UUID] = []

        if flight_node_ids:
            shipments_result = session.execute(
                text("""
                    SELECT DISTINCT e.src
                    FROM edge e
                    WHERE e.type = 'SHIPMENT_ON_FLIGHT'
                      AND e.source_system = 'SIMULATION'
                      AND e.dst = ANY(:flight_ids)
                """),
                {"flight_ids": flight_node_ids},
            )
            shipment_node_ids = [row[0] for row in shipments_result]

            carriers_result = session.execute(
                text("""
                    SELECT DISTINCT e.src
                    FROM edge e
                    WHERE e.type = 'CARRIER_OPERATES_FLIGHT'
                      AND e.source_system = 'SIMULATION'
                      AND e.dst = ANY(:flight_ids)
                """),
                {"flight_ids": flight_node_ids},
            )
            carrier_node_ids = [row[0] for row in carriers_result]

        if shipment_node_ids:
            bookings_result = session.execute(
                text("""
                    SELECT DISTINCT e.src
                    FROM edge e
                    WHERE e.type = 'BOOKING_FOR_SHIPMENT'
                      AND e.source_system = 'SIMULATION'
                      AND e.dst = ANY(:shipment_ids)
                """),
                {"shipment_ids": shipment_node_ids},
            )
            booking_node_ids = [row[0] for row in bookings_result]

            documents_result = session.execute(
                text("""
                    SELECT DISTINCT e.src
                    FROM edge e
                    WHERE e.type = 'DOCUMENT_FOR_SHIPMENT'
                      AND e.source_system = 'SIMULATION'
                      AND e.dst = ANY(:shipment_ids)
                """),
                {"shipment_ids": shipment_node_ids},
            )
            document_node_ids = [row[0] for row in documents_result]

        node_ids = list({
            *flight_node_ids,
            *shipment_node_ids,
            *booking_node_ids,
            *document_node_ids,
            *carrier_node_ids,
        })

        if not node_ids:
            return {
                "airport": airport_icao,
                "status": "noop",
                "edges_deleted": 0,
                "nodes_deleted": 0,
                "nodes_deleted_by_type": {},
            }

        # Delete SIMULATION edges touching these nodes (do not touch real integrations).
        deleted_edges = session.execute(
            text("""
                DELETE FROM edge
                WHERE source_system = 'SIMULATION'
                  AND (src = ANY(:node_ids) OR dst = ANY(:node_ids))
                RETURNING id
            """),
            {"node_ids": node_ids},
        ).fetchall()
        edge_ids = [row[0] for row in deleted_edges]

        if edge_ids:
            session.execute(
                text("DELETE FROM edge_evidence WHERE edge_id = ANY(:edge_ids)"),
                {"edge_ids": edge_ids},
            )

        # Delete only operational nodes that are now orphaned.
        orphan_result = session.execute(
            text("""
                SELECT n.id, n.type
                FROM node n
                WHERE n.id = ANY(:node_ids)
                  AND n.type = ANY(:node_types)
                  AND NOT EXISTS (SELECT 1 FROM edge e WHERE e.src = n.id OR e.dst = n.id)
                  AND NOT EXISTS (SELECT 1 FROM claim c WHERE c.subject_node_id = n.id)
            """),
            {"node_ids": node_ids, "node_types": list(OPERATIONAL_NODE_TYPES)},
        )
        orphan_rows = orphan_result.fetchall()
        orphan_ids = [row[0] for row in orphan_rows]

        nodes_deleted_by_type: Dict[str, int] = {}
        for _node_id, node_type in orphan_rows:
            nodes_deleted_by_type[node_type] = nodes_deleted_by_type.get(node_type, 0) + 1

        if orphan_ids:
            session.execute(
                text("DELETE FROM node_version WHERE node_id = ANY(:ids)"),
                {"ids": orphan_ids},
            )
            session.execute(
                text("DELETE FROM node WHERE id = ANY(:ids)"),
                {"ids": orphan_ids},
            )

        if commit:
            session.commit()

        return {
            "airport": airport_icao,
            "status": "success",
            "edges_deleted": len(edge_ids),
            "nodes_deleted": len(orphan_ids),
            "nodes_deleted_by_type": nodes_deleted_by_type,
        }
    finally:
        if owns_session:
            session.close()


def seed_graph_for_airport(
    airport_icao: str,
    num_flights: int = 20,
    shipments_per_flight: int = 5,
    seed: Optional[int] = None,
    session: Optional[Session] = None,
    *,
    bulk: bool = True,
    commit: bool = True,
) -> Dict[str, Any]:
    """
    Convenience function to seed graph for an airport.

    Args:
        airport_icao: Airport ICAO code
        num_flights: Number of flights to generate
        shipments_per_flight: Shipments per flight

    Returns:
        Summary of seeded data
    """
    from app.db.engine import SessionLocal

    owns_session = session is None
    if session is None:
        session = SessionLocal()

    try:
        graph_store = GraphStore(session)
        seeder = GraphSeeder(graph_store=graph_store, seed=seed)
        # If we create the session, always commit so the data persists.
        commit_effective = True if owns_session else commit
        return seeder.seed_airport(
            airport_icao,
            num_flights=num_flights,
            shipments_per_flight=shipments_per_flight,
            bulk=bulk,
            commit=commit_effective,
        )
    finally:
        if owns_session:
            session.close()

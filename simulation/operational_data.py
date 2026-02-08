# simulation/operational_data.py
"""
Simulated operational data generator.

Creates OPERATIONAL entities that work alongside real disruption signals:
- CARRIER nodes (airlines operating routes)
- FLIGHT nodes (connecting airports)
- SHIPMENT nodes (cargo being transported)
- BOOKING nodes (booking evidence for shipment actions)
- DOCUMENT nodes (customs docs, BOLs, etc.)

This enables:
1. Graph traversal: Airport disruption -> affected flights -> affected shipments
2. Shipment-level actions with booking evidence
3. Cascade analysis across the supply chain network
"""

import random
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any
from uuid import UUID, uuid4
from dataclasses import dataclass, field


@dataclass
class SimulatedCarrier:
    """Simulated airline/carrier."""
    id: str
    name: str
    iata_code: str
    hub_airports: List[str]  # ICAO codes


@dataclass
class SimulatedFlight:
    """Simulated flight connecting airports."""
    id: str
    flight_number: str
    carrier_id: str
    origin_icao: str
    destination_icao: str
    scheduled_departure: datetime
    scheduled_arrival: datetime
    status: str  # SCHEDULED, DELAYED, CANCELLED, IN_FLIGHT, ARRIVED
    aircraft_type: str


@dataclass
class SimulatedShipment:
    """Simulated cargo shipment."""
    id: str
    tracking_number: str
    flight_id: str
    origin_icao: str
    destination_icao: str
    weight_kg: float
    pieces: int
    commodity: str
    shipper: str
    consignee: str
    service_level: str  # STANDARD, EXPRESS, PREMIUM
    status: str  # BOOKED, TENDERED, IN_TRANSIT, DELIVERED, HELD


@dataclass
class SimulatedBooking:
    """Simulated booking (evidence for shipment actions)."""
    id: str
    booking_reference: str
    shipment_id: str
    flight_id: str
    carrier_id: str
    booked_at: datetime
    customer_id: str
    sla_deadline: datetime
    rate_type: str  # SPOT, CONTRACT, CHARTER
    # Forwarder revenue data (what forwarder actually knows)
    rate_per_kg: float  # $/kg charged to customer
    total_charge_usd: float  # Total freight charge (rate_per_kg * weight)
    margin_percent: float  # Forwarder margin on this booking


@dataclass
class SimulatedDocument:
    """Simulated shipping document."""
    id: str
    document_type: str  # AWB, HAWB, COMMERCIAL_INVOICE, CUSTOMS_DECLARATION
    document_number: str
    shipment_id: str
    issued_at: datetime
    status: str  # DRAFT, ISSUED, SUBMITTED, CLEARED
    deadline: Optional[datetime] = None


# Pre-defined carriers
CARRIERS = [
    SimulatedCarrier("CAR001", "United Cargo", "UA", ["KJFK", "KORD", "KLAX", "KDEN"]),
    SimulatedCarrier("CAR002", "American Airlines Cargo", "AA", ["KDFW", "KMIA", "KJFK", "KLAX"]),
    SimulatedCarrier("CAR003", "Delta Cargo", "DL", ["KATL", "KJFK", "KLAX", "KMSP"]),
    SimulatedCarrier("CAR004", "FedEx Express", "FX", ["KMEM", "KIND", "KOAK", "KEWR"]),
    SimulatedCarrier("CAR005", "UPS Airlines", "5X", ["KSDF", "KONT", "KPHL", "KDFW"]),
    SimulatedCarrier("CAR006", "Atlas Air", "5Y", ["KJFK", "KLAX", "KMIA", "KORD"]),
    SimulatedCarrier("CAR007", "Polar Air Cargo", "PO", ["KLAX", "PANC", "KSFO"]),
    SimulatedCarrier("CAR008", "Cargolux", "CV", ["KJFK", "KORD", "KLAX", "KMIA"]),
]

# Common commodities
COMMODITIES = [
    "Electronics", "Pharmaceuticals", "Automotive Parts", "Textiles",
    "Machinery", "Perishables", "Medical Equipment", "Consumer Goods",
    "Aerospace Components", "Chemicals", "Precious Metals", "Documents"
]

# Service levels with SLA hours
SERVICE_LEVELS = {
    "STANDARD": 96,  # 4 days
    "EXPRESS": 48,   # 2 days
    "PREMIUM": 24,   # 1 day
}


class OperationalDataGenerator:
    """
    Generates simulated operational data for the supply chain graph.

    Creates carriers, flights, shipments, bookings, and documents
    that connect to AIRPORT nodes via graph edges.
    """

    def __init__(self, seed: Optional[int] = None):
        # Use an instance RNG so runs are deterministic without mutating global state.
        self.seed = seed
        self.rng = random.Random(seed)

        self.carriers = CARRIERS.copy()
        self.flights: List[SimulatedFlight] = []
        self.shipments: List[SimulatedShipment] = []
        self.bookings: List[SimulatedBooking] = []
        self.documents: List[SimulatedDocument] = []

    def generate_flights_for_airport(
        self,
        airport_icao: str,
        num_departures: int = 10,
        num_arrivals: int = 10,
        base_time: Optional[datetime] = None
    ) -> List[SimulatedFlight]:
        """
        Generate simulated flights for an airport.

        Args:
            airport_icao: Airport ICAO code
            num_departures: Number of departing flights
            num_arrivals: Number of arriving flights
            base_time: Base time for scheduling (defaults to now)

        Returns:
            List of generated flights
        """
        if base_time is None:
            base_time = datetime.now(timezone.utc)

        flights = []
        # For a more realistic demo, generate some flights slightly in the past so:
        # - there are always in-flight/arrived legs,
        # - SLA deadlines aren't uniformly far in the future,
        # - the UI shows a mix of "imminent" vs "safe" exposure.
        schedule_window_start_hours = -6
        schedule_window_end_hours = 24

        def derive_status(departure_time: datetime, arrival_time: datetime) -> str:
            if arrival_time <= base_time:
                # Already landed (or should have)
                return self.rng.choice(["ARRIVED", "ARRIVED", "ARRIVED", "DELAYED"])
            if departure_time <= base_time < arrival_time:
                return self.rng.choice(["IN_FLIGHT", "IN_FLIGHT", "DELAYED"])
            return self.rng.choice(["SCHEDULED", "SCHEDULED", "DELAYED"])

        # Major US airports for connections
        major_airports = [
            "KJFK", "KLAX", "KORD", "KATL", "KDFW", "KDEN", "KSFO",
            "KMIA", "KSEA", "KBOS", "KEWR", "KPHL", "KIAD", "KPHX"
        ]
        destinations = [a for a in major_airports if a != airport_icao]

        # Generate departing flights
        for i in range(num_departures):
            carrier = self.rng.choice(self.carriers)
            dest = self.rng.choice(destinations)

            departure = base_time + timedelta(hours=self.rng.uniform(schedule_window_start_hours, schedule_window_end_hours))
            flight_hours = self.rng.uniform(1, 6)
            arrival = departure + timedelta(hours=flight_hours)

            flight = SimulatedFlight(
                id=f"FLT{uuid4().hex[:8].upper()}",
                flight_number=f"{carrier.iata_code}{self.rng.randint(100, 9999)}",
                carrier_id=carrier.id,
                origin_icao=airport_icao,
                destination_icao=dest,
                scheduled_departure=departure,
                scheduled_arrival=arrival,
                status=derive_status(departure, arrival),
                aircraft_type=self.rng.choice(["B777F", "B747F", "B767F", "A330F", "MD11F"])
            )
            flights.append(flight)

        # Generate arriving flights
        for i in range(num_arrivals):
            carrier = self.rng.choice(self.carriers)
            origin = self.rng.choice(destinations)

            arrival = base_time + timedelta(hours=self.rng.uniform(schedule_window_start_hours, schedule_window_end_hours))
            flight_hours = self.rng.uniform(1, 6)
            departure = arrival - timedelta(hours=flight_hours)

            flight = SimulatedFlight(
                id=f"FLT{uuid4().hex[:8].upper()}",
                flight_number=f"{carrier.iata_code}{self.rng.randint(100, 9999)}",
                carrier_id=carrier.id,
                origin_icao=origin,
                destination_icao=airport_icao,
                scheduled_departure=departure,
                scheduled_arrival=arrival,
                status=derive_status(departure, arrival),
                aircraft_type=self.rng.choice(["B777F", "B747F", "B767F", "A330F", "MD11F"])
            )
            flights.append(flight)

        self.flights.extend(flights)
        return flights

    def generate_shipments_for_flight(
        self,
        flight: SimulatedFlight,
        num_shipments: int = 5
    ) -> List[SimulatedShipment]:
        """
        Generate simulated shipments on a flight.

        Args:
            flight: Flight to add shipments to
            num_shipments: Number of shipments to generate

        Returns:
            List of generated shipments
        """
        shipments = []

        shippers = [
            "Acme Logistics", "Global Trade Co", "FastShip Inc",
            "Prime Cargo", "Swift Freight", "Alliance Transport"
        ]
        consignees = [
            "Tech Distributors", "MedSupply Corp", "AutoParts Ltd",
            "Consumer Direct", "Industrial Solutions", "Retail Hub"
        ]

        for i in range(num_shipments):
            service_level = self.rng.choice(list(SERVICE_LEVELS.keys()))

            shipment = SimulatedShipment(
                id=f"SHP{uuid4().hex[:8].upper()}",
                tracking_number=f"{self.rng.randint(100000000, 999999999)}",
                flight_id=flight.id,
                origin_icao=flight.origin_icao,
                destination_icao=flight.destination_icao,
                weight_kg=round(self.rng.uniform(10, 5000), 1),
                pieces=self.rng.randint(1, 50),
                commodity=self.rng.choice(COMMODITIES),
                shipper=self.rng.choice(shippers),
                consignee=self.rng.choice(consignees),
                service_level=service_level,
                status=self.rng.choice(["BOOKED", "TENDERED", "IN_TRANSIT"])
            )
            shipments.append(shipment)

        self.shipments.extend(shipments)
        return shipments

    def generate_booking_for_shipment(
        self,
        shipment: SimulatedShipment,
        flight: SimulatedFlight,
        carrier: SimulatedCarrier
    ) -> SimulatedBooking:
        """
        Generate booking record for a shipment (evidence for shipment actions).

        Args:
            shipment: Shipment to create booking for
            flight: Flight the shipment is on
            carrier: Carrier operating the flight

        Returns:
            Generated booking
        """
        sla_hours = SERVICE_LEVELS[shipment.service_level]
        booked_at = flight.scheduled_departure - timedelta(hours=self.rng.uniform(24, 72))
        sla_deadline = flight.scheduled_arrival + timedelta(hours=sla_hours)

        # Realistic air freight rates (what forwarder actually charges)
        # Base rates per kg by service level
        base_rates = {
            "STANDARD": (2.50, 5.00),   # $2.50-5.00/kg
            "EXPRESS": (5.00, 10.00),   # $5.00-10.00/kg
            "PREMIUM": (10.00, 20.00),  # $10.00-20.00/kg
        }
        rate_range = base_rates.get(shipment.service_level, (3.00, 7.00))

        # Rate type affects pricing (SPOT is higher than CONTRACT)
        rate_type = self.rng.choice(["SPOT", "CONTRACT", "CONTRACT", "CONTRACT"])
        rate_multiplier = 1.3 if rate_type == "SPOT" else 1.0

        rate_per_kg = round(self.rng.uniform(*rate_range) * rate_multiplier, 2)
        total_charge = round(rate_per_kg * shipment.weight_kg, 2)

        # Forwarder margin (typically 15-30%)
        margin_percent = round(self.rng.uniform(15, 30), 1)

        booking = SimulatedBooking(
            id=f"BKG{uuid4().hex[:8].upper()}",
            booking_reference=f"BK{self.rng.randint(1000000, 9999999)}",
            shipment_id=shipment.id,
            flight_id=flight.id,
            carrier_id=carrier.id,
            booked_at=booked_at,
            customer_id=f"CUST{self.rng.randint(1000, 9999)}",
            sla_deadline=sla_deadline,
            rate_type=rate_type,
            rate_per_kg=rate_per_kg,
            total_charge_usd=total_charge,
            margin_percent=margin_percent,
        )

        self.bookings.append(booking)
        return booking

    def generate_documents_for_shipment(
        self,
        shipment: SimulatedShipment
    ) -> List[SimulatedDocument]:
        """
        Generate shipping documents for a shipment.

        Args:
            shipment: Shipment to create documents for

        Returns:
            List of generated documents
        """
        documents = []
        now = datetime.now(timezone.utc)

        # Air Waybill (required)
        awb = SimulatedDocument(
            id=f"DOC{uuid4().hex[:8].upper()}",
            document_type="AWB",
            document_number=f"{self.rng.randint(100, 999)}-{shipment.tracking_number}",
            shipment_id=shipment.id,
            issued_at=now - timedelta(hours=self.rng.uniform(24, 72)),
            status="ISSUED"
        )
        documents.append(awb)

        # House Air Waybill (consolidation)
        if self.rng.random() > 0.3:
            hawb = SimulatedDocument(
                id=f"DOC{uuid4().hex[:8].upper()}",
                document_type="HAWB",
                document_number=f"H{self.rng.randint(10000000, 99999999)}",
                shipment_id=shipment.id,
                issued_at=now - timedelta(hours=self.rng.uniform(12, 48)),
                status="ISSUED"
            )
            documents.append(hawb)

        # Commercial Invoice
        invoice = SimulatedDocument(
            id=f"DOC{uuid4().hex[:8].upper()}",
            document_type="COMMERCIAL_INVOICE",
            document_number=f"INV{self.rng.randint(100000, 999999)}",
            shipment_id=shipment.id,
            issued_at=now - timedelta(hours=self.rng.uniform(24, 96)),
            status="ISSUED"
        )
        documents.append(invoice)

        # Customs Declaration (may have deadline)
        customs_deadline = now + timedelta(hours=self.rng.uniform(12, 48))
        customs = SimulatedDocument(
            id=f"DOC{uuid4().hex[:8].upper()}",
            document_type="CUSTOMS_DECLARATION",
            document_number=f"CUS{self.rng.randint(100000, 999999)}",
            shipment_id=shipment.id,
            issued_at=now - timedelta(hours=self.rng.uniform(6, 24)),
            status=self.rng.choice(["DRAFT", "SUBMITTED", "CLEARED"]),
            deadline=customs_deadline
        )
        documents.append(customs)

        self.documents.extend(documents)
        return documents

    def generate_full_dataset_for_airport(
        self,
        airport_icao: str,
        num_flights: int = 20,
        shipments_per_flight: int = 5
    ) -> Dict[str, Any]:
        """
        Generate complete operational dataset for an airport.

        Creates flights, shipments, bookings, and documents
        all connected to the specified airport.

        Args:
            airport_icao: Airport ICAO code
            num_flights: Number of flights to generate
            shipments_per_flight: Shipments per flight

        Returns:
            Dictionary with all generated data
        """
        # Generate flights (half departures, half arrivals)
        flights = self.generate_flights_for_airport(
            airport_icao,
            num_departures=num_flights // 2,
            num_arrivals=num_flights // 2
        )

        all_shipments = []
        all_bookings = []
        all_documents = []

        for flight in flights:
            # Find carrier for this flight
            carrier = next((c for c in self.carriers if c.id == flight.carrier_id), self.carriers[0])

            # Generate shipments
            shipments = self.generate_shipments_for_flight(flight, shipments_per_flight)

            for shipment in shipments:
                # Generate booking (evidence)
                booking = self.generate_booking_for_shipment(shipment, flight, carrier)
                all_bookings.append(booking)

                # Generate documents
                docs = self.generate_documents_for_shipment(shipment)
                all_documents.extend(docs)

            all_shipments.extend(shipments)

        return {
            "airport": airport_icao,
            "carriers": self.carriers,
            "flights": flights,
            "shipments": all_shipments,
            "bookings": all_bookings,
            "documents": all_documents,
            "stats": {
                "flight_count": len(flights),
                "shipment_count": len(all_shipments),
                "booking_count": len(all_bookings),
                "document_count": len(all_documents),
                "total_revenue_usd": sum(b.total_charge_usd for b in all_bookings),
                "total_weight_kg": sum(s.weight_kg for s in all_shipments),
            }
        }

    def get_carrier_by_id(self, carrier_id: str) -> Optional[SimulatedCarrier]:
        """Get carrier by ID."""
        return next((c for c in self.carriers if c.id == carrier_id), None)


def stable_seed_from_string(value: str) -> int:
    """Create a stable 32-bit seed from a string."""
    import hashlib

    digest = hashlib.sha256(value.encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big")


def default_operational_seed_for_airport(airport_icao: str) -> int:
    """Deterministic seed used for operational graph seeding."""
    return stable_seed_from_string(f"ops:{airport_icao.upper()}")

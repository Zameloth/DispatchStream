"""Shared configuration for the DispatchStream dashboard.

Mirrors the env-var and topic-naming conventions already used by
Topics.java and scripts/rejouer.py: GROUPE prefixes every output topic,
KAFKA_BOOTSTRAP points at the cluster (docker-compose exposes it on
localhost:29092).
"""
import os

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
GROUPE = os.environ.get("GROUPE", "grp00")

# Entrees (fournies, lecture seule)
RIDE_REQUESTS = "dispatch.ride.requests"
DRIVER_LOCATIONS = "dispatch.driver.locations"
TRIP_EVENTS = "dispatch.trip.events"


def out(suffix: str) -> str:
    return f"{GROUPE}.{suffix}"


# Sorties (produites par le pipeline, prefixees par GROUPE)
DLQ = out("dispatch.dlq")
DEMAND_BY_ZONE = out("dispatch.demand.by-zone")
SUPPLY_BY_ZONE = out("dispatch.supply.by-zone")
SURGE = out("dispatch.surge")
ALERTS_TRIPS = out("dispatch.alerts.trips")

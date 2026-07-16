package fr.esgi.kafka.dispatch.stateful;

import fr.esgi.kafka.dispatch.model.DriverLocation;

/**
 * Derniere occurrence connue d'une identite (driver_id, timestamp),
 * memorisee par DriverLocationStatefulValidation pour la detection de
 * duplicats/conflits (cf. ADR 0007). seenAtEpochMilli est le timestamp
 * d'arrivee (record Kafka), pas le timestamp metier du ping.
 */
public record DriverLocationOccurrence(DriverLocation location, long seenAtEpochMilli) {
}

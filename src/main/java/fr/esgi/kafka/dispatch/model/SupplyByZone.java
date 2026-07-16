package fr.esgi.kafka.dispatch.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Valeur publiee sur Topics.SUPPLY_BY_ZONE (issue #7) : nombre de
 * chauffeurs AVAILABLE dans la zone, a l'instant de l'emission
 * (cf. CONTEXT.md "Supply" - derniere position connue par chauffeur).
 */
public record SupplyByZone(
        @JsonProperty("zone") String zone,
        @JsonProperty("available_drivers") long availableDrivers) {
}

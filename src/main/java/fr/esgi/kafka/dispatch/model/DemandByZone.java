package fr.esgi.kafka.dispatch.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Compte de demandes de course pour une zone sur une fenetre tumbling
 * (topic dispatch.demand.by-zone, cle = zone). Un couple (zone, fenetre)
 * peut apparaitre plusieurs fois sur le topic (emission continue, cf.
 * TopologyProducer.buildDemandByZone / ADR 0001).
 */
public record DemandByZone(
        @JsonProperty("zone") String zone,
        @JsonProperty("window_start") String windowStart,
        @JsonProperty("window_end") String windowEnd,
        @JsonProperty("count") long count) {
}

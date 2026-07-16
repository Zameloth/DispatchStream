package fr.esgi.kafka.dispatch.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Reponse de GET /api/v1/surge/{zone} (DISP-6, issue #10) : le ratio
 * courant d'une zone, lu depuis TopologyProducer.SURGE_RATIO_STORE (ADR
 * 0002), donc le ratio non filtre - contrairement a SurgeEvent qui ne
 * porte que les incidents ayant franchi le seuil. "surging" reexpose ce
 * meme seuil (> 2) cote API pour eviter au client de le recoder.
 */
public record SurgeStatus(
        @JsonProperty("zone") String zone,
        @JsonProperty("ratio") double ratio,
        @JsonProperty("surging") boolean surging) {
}

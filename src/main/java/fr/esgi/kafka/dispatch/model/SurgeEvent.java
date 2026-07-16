package fr.esgi.kafka.dispatch.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Valeur publiee sur Topics.SURGE (issue #8) : ratio demande/offre d'une
 * zone au moment ou il a franchi le seuil de surge (> 2, cf. CONTEXT.md
 * "Surge"). Seuls les zones en surge sont publiees ici ; le ratio de
 * chaque zone, filtre ou non, est materialise dans
 * TopologyProducer.SURGE_RATIO_STORE (ADR 0002).
 */
public record SurgeEvent(
        @JsonProperty("zone") String zone,
        @JsonProperty("ratio") double ratio) {
}

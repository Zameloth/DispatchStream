package fr.esgi.kafka.dispatch.common;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Enveloppe DLQ generique, utilisee par toute etape de validation du
 * pipeline (issues #2, #3, #4) qui route un message invalide vers
 * Topics.DLQ. Le message original (rawValue) est conserve intact, octet
 * pour octet (aucune reserialisation), pour permettre un rejeu/diagnostic
 * manuel a partir de la DLQ.
 * rejectedAt est une chaine ISO-8601 (et non un java.time.Instant) pour
 * rester coherent avec le reste du pipeline, qui represente deja tous
 * ses timestamps en String (cf. model.RideRequest et consorts), et pour
 * eviter toute dependance a un module Jackson JSR-310 non enregistre sur
 * le mapper "brut" partage (JsonSerdes).
 */
public record DlqEnvelope(
        @JsonProperty("source_topic") String sourceTopic,
        @JsonProperty("key") String key,
        @JsonProperty("raw_value") String rawValue,
        @JsonProperty("reason") String reason,
        @JsonProperty("rejected_at") String rejectedAt) {
}

package fr.esgi.kafka.dispatch.common;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;

/**
 * Verifie qu'un timestamp est parsable en ISO-8601 (cf. ADR 0004, derniere
 * regle de la chaine de validation). Partage par toute validation
 * d'evenement (RideRequest, DriverLocation, TripEvent...).
 */
public final class Timestamps {

    /**
     * Seuil de retard (CONTEXT.md "Late event") : aussi la duree de la
     * fenetre de dedup/retention orphan-END (meme borne, cf. ADR 0007 et
     * CONTEXT.md "Duplicate event"/"Orphan END event").
     */
    public static final Duration LATE_THRESHOLD = Duration.ofMinutes(6);

    private Timestamps() {
    }

    public static boolean isParseable(String timestamp) {
        if (timestamp == null) {
            return false;
        }
        try {
            Instant.parse(timestamp);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }

    /**
     * Vrai si l'evenement arrive plus de LATE_THRESHOLD apres son propre
     * timestamp (CONTEXT.md "Late event" : "arriving more than 6 minutes
     * after its own timestamp"). timestamp doit deja etre parsable
     * (verifie en amont par la chaine stateless, cf. ADR 0004).
     */
    public static boolean isLate(String timestamp, Instant arrivedAt) {
        Instant eventTime = Instant.parse(timestamp);
        return Duration.between(eventTime, arrivedAt).compareTo(LATE_THRESHOLD) > 0;
    }
}

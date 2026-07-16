package fr.esgi.kafka.dispatch.common;

import java.time.Instant;
import java.time.format.DateTimeParseException;

/**
 * Verifie qu'un timestamp est parsable en ISO-8601 (cf. ADR 0004, derniere
 * regle de la chaine de validation). Partage par toute validation
 * d'evenement (RideRequest, DriverLocation, TripEvent...).
 */
public final class Timestamps {

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
}

package fr.esgi.kafka.dispatch.validation;

import fr.esgi.kafka.dispatch.common.EventValidator;
import fr.esgi.kafka.dispatch.common.ValidationStep;
import fr.esgi.kafka.dispatch.common.Zones;
import fr.esgi.kafka.dispatch.model.DriverLocation;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Set;

/**
 * Regles de validation metier propres a DriverLocation (topic
 * dispatch.driver.locations), appliquees par EventValidator apres le
 * parsing generique en deux phases (cf. ADR 0004). Ordre fixe (ADR 0004
 * "Reason precedence") : champs requis -> enum zone/status -> bornes GPS
 * -> timestamp parsable. La premiere regle en echec fixe le reason code.
 */
public final class DriverLocationValidation {

    /** Statuts chauffeur valides (modele DriverLocation / CONTEXT.md). */
    public static final Set<String> KNOWN_STATUSES = Set.of("AVAILABLE", "BUSY", "OFFLINE");

    /** Bornes de validite GPS pour ce projet (README.md / CONTEXT.md). */
    private static final double MIN_LAT = 48.7;
    private static final double MAX_LAT = 49.0;
    private static final double MIN_LON = 2.2;
    private static final double MAX_LON = 2.5;

    public static final List<ValidationStep<DriverLocation>> STEPS = List.of(
            ValidationStep.of(d -> d.driverId() != null, "missing_field:driver_id"),
            ValidationStep.of(d -> d.lat() != null, "missing_field:lat"),
            ValidationStep.of(d -> d.lon() != null, "missing_field:lon"),
            ValidationStep.of(d -> d.zone() != null, "missing_field:zone"),
            ValidationStep.of(d -> d.status() != null, "missing_field:status"),
            ValidationStep.of(d -> d.timestamp() != null, "missing_field:timestamp"),
            ValidationStep.of(DriverLocationValidation::isKnownZone, "unknown_zone"),
            ValidationStep.of(DriverLocationValidation::isKnownStatus, "unknown_status"),
            ValidationStep.of(DriverLocationValidation::isWithinParisBounds, "gps_out_of_bounds"),
            ValidationStep.of(d -> isParseableTimestamp(d.timestamp()), "unparseable_timestamp"));

    public static final EventValidator<DriverLocation> VALIDATOR =
            new EventValidator<>(DriverLocation.class, STEPS);

    private DriverLocationValidation() {
    }

    private static boolean isKnownZone(DriverLocation d) {
        return d.zone() != null && Zones.KNOWN_ZONES.contains(d.zone());
    }

    private static boolean isKnownStatus(DriverLocation d) {
        return d.status() != null && KNOWN_STATUSES.contains(d.status());
    }

    private static boolean isWithinParisBounds(DriverLocation d) {
        if (d.lat() == null || d.lon() == null) {
            return false;
        }
        double lat = d.lat();
        double lon = d.lon();
        return lat >= MIN_LAT && lat <= MAX_LAT && lon >= MIN_LON && lon <= MAX_LON;
    }

    private static boolean isParseableTimestamp(String timestamp) {
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

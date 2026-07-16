package fr.esgi.kafka.dispatch.validation;

import fr.esgi.kafka.dispatch.common.EventValidator;
import fr.esgi.kafka.dispatch.common.ValidationStep;
import fr.esgi.kafka.dispatch.common.Zones;
import fr.esgi.kafka.dispatch.model.RideRequest;
import fr.esgi.kafka.dispatch.model.RideRequest.GeoPoint;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Regles de validation metier propres a RideRequest (topic
 * dispatch.ride.requests), appliquees par EventValidator apres le
 * parsing generique en deux phases (cf. ADR 0004). Ordre fixe (ADR 0004
 * "Reason precedence") : champs requis -> enum zone -> bornes GPS ->
 * timestamp parsable. La premiere regle en echec fixe le reason code.
 */
public final class RideRequestValidation {

    /**
     * Les 12 zones fixes de Paris. Centralisees dans common.Zones (issue
     * #3) car DriverLocationValidation en a desormais besoin aussi ; ce
     * champ est conserve pour compatibilite ascendante de l'API publique.
     */
    public static final Set<String> KNOWN_ZONES = Zones.KNOWN_ZONES;

    /** Bornes de validite GPS pour ce projet (README.md / CONTEXT.md). */
    private static final double MIN_LAT = 48.7;
    private static final double MAX_LAT = 49.0;
    private static final double MIN_LON = 2.2;
    private static final double MAX_LON = 2.5;

    public static final List<ValidationStep<RideRequest>> STEPS = List.of(
            ValidationStep.of(r -> r.requestId() != null, "missing_field:request_id"),
            ValidationStep.of(r -> r.userId() != null, "missing_field:user_id"),
            ValidationStep.of(r -> r.pickup() != null, "missing_field:pickup"),
            ValidationStep.of(r -> r.dropoff() != null, "missing_field:dropoff"),
            ValidationStep.of(r -> r.timestamp() != null, "missing_field:timestamp"),
            ValidationStep.of(r -> hasField(r.pickup(), GeoPoint::lat), "missing_field:pickup.lat"),
            ValidationStep.of(r -> hasField(r.pickup(), GeoPoint::lon), "missing_field:pickup.lon"),
            ValidationStep.of(r -> hasField(r.pickup(), GeoPoint::zone), "missing_field:pickup.zone"),
            ValidationStep.of(r -> hasField(r.dropoff(), GeoPoint::lat), "missing_field:dropoff.lat"),
            ValidationStep.of(r -> hasField(r.dropoff(), GeoPoint::lon), "missing_field:dropoff.lon"),
            ValidationStep.of(r -> hasField(r.dropoff(), GeoPoint::zone), "missing_field:dropoff.zone"),
            ValidationStep.of(r -> isKnownZone(r.pickup()), "unknown_zone:pickup"),
            ValidationStep.of(r -> isKnownZone(r.dropoff()), "unknown_zone:dropoff"),
            ValidationStep.of(r -> isWithinParisBounds(r.pickup()), "gps_out_of_bounds:pickup"),
            ValidationStep.of(r -> isWithinParisBounds(r.dropoff()), "gps_out_of_bounds:dropoff"),
            ValidationStep.of(r -> isParseableTimestamp(r.timestamp()), "unparseable_timestamp"));

    public static final EventValidator<RideRequest> VALIDATOR =
            new EventValidator<>(RideRequest.class, STEPS);

    private RideRequestValidation() {
    }

    private static boolean hasField(GeoPoint point, Function<GeoPoint, Object> accessor) {
        return point != null && accessor.apply(point) != null;
    }

    private static boolean isKnownZone(GeoPoint point) {
        return point != null && point.zone() != null && KNOWN_ZONES.contains(point.zone());
    }

    private static boolean isWithinParisBounds(GeoPoint point) {
        if (point == null || point.lat() == null || point.lon() == null) {
            return false;
        }
        double lat = point.lat();
        double lon = point.lon();
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

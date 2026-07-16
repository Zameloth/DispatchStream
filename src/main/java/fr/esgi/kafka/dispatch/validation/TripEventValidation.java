package fr.esgi.kafka.dispatch.validation;

import fr.esgi.kafka.dispatch.common.EventValidator;
import fr.esgi.kafka.dispatch.common.Timestamps;
import fr.esgi.kafka.dispatch.common.ValidationStep;
import fr.esgi.kafka.dispatch.model.TripEvent;

import java.util.List;
import java.util.Set;

/**
 * Regles de validation metier propres a TripEvent (topic
 * dispatch.trip.events), appliquees par EventValidator apres le parsing
 * generique en deux phases (cf. ADR 0004). Portee volontairement sans
 * etat (cf. ADR 0003) : la correlation d'un END avec un START anterieur
 * (detection des "orphan END") est explicitement hors-perimetre de cette
 * validation et deferree a un ticket ulterieur.
 */
public final class TripEventValidation {

    private static final Set<String> KNOWN_EVENTS = Set.of("START", "END");

    public static final List<ValidationStep<TripEvent>> STEPS = List.of(
            ValidationStep.of(t -> t.tripId() != null, "missing_field:trip_id"),
            ValidationStep.of(t -> t.driverId() != null, "missing_field:driver_id"),
            ValidationStep.of(t -> t.userId() != null, "missing_field:user_id"),
            ValidationStep.of(t -> t.event() != null, "missing_field:event"),
            ValidationStep.of(t -> t.timestamp() != null, "missing_field:timestamp"),
            ValidationStep.of(t -> !isEnd(t) || t.distanceKm() != null, "missing_field:distance_km"),
            ValidationStep.of(t -> !isEnd(t) || t.durationSeconds() != null, "missing_field:duration_seconds"),
            ValidationStep.of(t -> t.event() == null || KNOWN_EVENTS.contains(t.event()), "unknown_event"),
            ValidationStep.of(t -> t.distanceKm() == null || t.distanceKm() >= 0, "invalid_distance_km"),
            ValidationStep.of(t -> t.durationSeconds() == null || t.durationSeconds() > 0, "invalid_duration_seconds"),
            ValidationStep.of(t -> Timestamps.isParseable(t.timestamp()), "unparseable_timestamp"));

    public static final EventValidator<TripEvent> VALIDATOR =
            new EventValidator<>(TripEvent.class, STEPS);

    private TripEventValidation() {
    }

    private static boolean isEnd(TripEvent t) {
        return "END".equals(t.event());
    }
}

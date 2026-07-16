package fr.esgi.kafka.dispatch.validation;

import fr.esgi.kafka.dispatch.common.ValidationOutcome;
import fr.esgi.kafka.dispatch.model.TripEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Verifie TripEventValidation.VALIDATOR : chaque test correspond a un
 * critere d'acceptation de l'issue #4 (parse deux phases + chaine de
 * regles metier a court-circuit, cf. ADR 0004).
 */
class TripEventValidationTest {

    @Test
    void truncatedJsonIsRejectedAsMalformedJson() {
        String raw = "{\"trip_id\": \"trip-1";

        ValidationOutcome<TripEvent> outcome = TripEventValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<TripEvent> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("malformed_json", invalid.reason());
        assertEquals(raw, invalid.raw());
    }

    @Test
    void emptyMessageIsRejectedAsMalformedJson() {
        String raw = "";

        ValidationOutcome<TripEvent> outcome = TripEventValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<TripEvent> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("malformed_json", invalid.reason());
    }

    @Test
    void wellFormedJsonWithWrongFieldTypeIsRejectedWithFieldSpecificReason() {
        String raw = """
                {
                  "trip_id": "trip-1",
                  "driver_id": "drv-1",
                  "user_id": "u-1",
                  "event": "START",
                  "distance_km": "loin",
                  "timestamp": "2026-07-16T10:00:00Z"
                }
                """;

        ValidationOutcome<TripEvent> outcome = TripEventValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<TripEvent> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("wrong_type:distance_km", invalid.reason());
    }

    @Test
    void missingTripIdIsRejectedWithFieldSpecificReason() {
        String raw = """
                {
                  "driver_id": "drv-1",
                  "user_id": "u-1",
                  "event": "START",
                  "timestamp": "2026-07-16T10:00:00Z"
                }
                """;

        ValidationOutcome<TripEvent> outcome = TripEventValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<TripEvent> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("missing_field:trip_id", invalid.reason());
    }

    @Test
    void missingDriverIdIsRejectedWithFieldSpecificReason() {
        String raw = """
                {
                  "trip_id": "trip-1",
                  "user_id": "u-1",
                  "event": "START",
                  "timestamp": "2026-07-16T10:00:00Z"
                }
                """;

        ValidationOutcome<TripEvent> outcome = TripEventValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<TripEvent> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("missing_field:driver_id", invalid.reason());
    }

    @Test
    void missingUserIdIsRejectedWithFieldSpecificReason() {
        String raw = """
                {
                  "trip_id": "trip-1",
                  "driver_id": "drv-1",
                  "event": "START",
                  "timestamp": "2026-07-16T10:00:00Z"
                }
                """;

        ValidationOutcome<TripEvent> outcome = TripEventValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<TripEvent> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("missing_field:user_id", invalid.reason());
    }

    @Test
    void missingEventIsRejectedWithFieldSpecificReason() {
        String raw = """
                {
                  "trip_id": "trip-1",
                  "driver_id": "drv-1",
                  "user_id": "u-1",
                  "timestamp": "2026-07-16T10:00:00Z"
                }
                """;

        ValidationOutcome<TripEvent> outcome = TripEventValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<TripEvent> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("missing_field:event", invalid.reason());
    }

    @Test
    void missingTimestampIsRejectedWithFieldSpecificReason() {
        String raw = """
                {
                  "trip_id": "trip-1",
                  "driver_id": "drv-1",
                  "user_id": "u-1",
                  "event": "START"
                }
                """;

        ValidationOutcome<TripEvent> outcome = TripEventValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<TripEvent> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("missing_field:timestamp", invalid.reason());
    }

    @Test
    void wellFormedStartTripEventIsValidWithoutDistanceOrDuration() {
        String raw = """
                {
                  "trip_id": "trip-1",
                  "driver_id": "drv-1",
                  "user_id": "u-1",
                  "event": "START",
                  "timestamp": "2026-07-16T10:00:00Z"
                }
                """;

        ValidationOutcome<TripEvent> outcome = TripEventValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Valid<TripEvent> valid =
                assertInstanceOf(ValidationOutcome.Valid.class, outcome);
        assertEquals("trip-1", valid.value().tripId());
        assertEquals("START", valid.value().event());
        assertEquals(raw, valid.raw());
    }

    @Test
    void unrecognizedEventValueIsRejected() {
        String raw = """
                {
                  "trip_id": "trip-1",
                  "driver_id": "drv-1",
                  "user_id": "u-1",
                  "event": "PAUSE",
                  "timestamp": "2026-07-16T10:00:00Z"
                }
                """;

        ValidationOutcome<TripEvent> outcome = TripEventValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<TripEvent> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("unknown_event", invalid.reason());
    }

    @Test
    void wellFormedEndTripEventIsValid() {
        String raw = """
                {
                  "trip_id": "trip-1",
                  "driver_id": "drv-1",
                  "user_id": "u-1",
                  "event": "END",
                  "distance_km": 4.2,
                  "duration_seconds": 620,
                  "timestamp": "2026-07-16T10:12:00Z"
                }
                """;

        ValidationOutcome<TripEvent> outcome = TripEventValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Valid<TripEvent> valid =
                assertInstanceOf(ValidationOutcome.Valid.class, outcome);
        assertEquals("END", valid.value().event());
        assertEquals(4.2, valid.value().distanceKm());
        assertEquals(620, valid.value().durationSeconds());
    }

    @Test
    void endEventMissingDistanceKmIsRejected() {
        String raw = """
                {
                  "trip_id": "trip-1",
                  "driver_id": "drv-1",
                  "user_id": "u-1",
                  "event": "END",
                  "duration_seconds": 620,
                  "timestamp": "2026-07-16T10:12:00Z"
                }
                """;

        ValidationOutcome<TripEvent> outcome = TripEventValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<TripEvent> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("missing_field:distance_km", invalid.reason());
    }

    @Test
    void endEventMissingDurationSecondsIsRejected() {
        String raw = """
                {
                  "trip_id": "trip-1",
                  "driver_id": "drv-1",
                  "user_id": "u-1",
                  "event": "END",
                  "distance_km": 4.2,
                  "timestamp": "2026-07-16T10:12:00Z"
                }
                """;

        ValidationOutcome<TripEvent> outcome = TripEventValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<TripEvent> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("missing_field:duration_seconds", invalid.reason());
    }

    @Test
    void endEventWithNegativeDistanceKmIsRejected() {
        String raw = """
                {
                  "trip_id": "trip-1",
                  "driver_id": "drv-1",
                  "user_id": "u-1",
                  "event": "END",
                  "distance_km": -1.5,
                  "duration_seconds": 620,
                  "timestamp": "2026-07-16T10:12:00Z"
                }
                """;

        ValidationOutcome<TripEvent> outcome = TripEventValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<TripEvent> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("invalid_distance_km", invalid.reason());
    }

    @Test
    void endEventWithNonPositiveDurationSecondsIsRejected() {
        String raw = """
                {
                  "trip_id": "trip-1",
                  "driver_id": "drv-1",
                  "user_id": "u-1",
                  "event": "END",
                  "distance_km": 4.2,
                  "duration_seconds": 0,
                  "timestamp": "2026-07-16T10:12:00Z"
                }
                """;

        ValidationOutcome<TripEvent> outcome = TripEventValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<TripEvent> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("invalid_duration_seconds", invalid.reason());
    }

    @Test
    void unparseableTimestampIsRejected() {
        String raw = """
                {
                  "trip_id": "trip-1",
                  "driver_id": "drv-1",
                  "user_id": "u-1",
                  "event": "START",
                  "timestamp": "hier a 15h"
                }
                """;

        ValidationOutcome<TripEvent> outcome = TripEventValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<TripEvent> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("unparseable_timestamp", invalid.reason());
    }

    @Test
    void orphanEndEventWithNoPriorStartIsNotRejectedByThisValidator() {
        // Correlation avec un eventuel START anterieur est hors-perimetre
        // de cette validation stateless (cf. ADR 0003) : un END bien forme,
        // meme "orphelin", doit passer.
        String raw = """
                {
                  "trip_id": "trip-orphan",
                  "driver_id": "drv-1",
                  "user_id": "u-1",
                  "event": "END",
                  "distance_km": 4.2,
                  "duration_seconds": 620,
                  "timestamp": "2026-07-16T10:12:00Z"
                }
                """;

        ValidationOutcome<TripEvent> outcome = TripEventValidation.VALIDATOR.validate(raw);

        assertInstanceOf(ValidationOutcome.Valid.class, outcome);
    }

    @Test
    void conditionalRequiredFieldTakesPrecedenceOverEnumMembership() {
        // event ni START ni END (echouerait unknown_event) ET distance_km
        // absent (echouerait missing_field:distance_km si event valait
        // END) : l'ordre fixe place les champs requis (dont les champs
        // conditionnels END) avant la verification d'enum, donc
        // missing_field:distance_km doit remonter en premier.
        String raw = """
                {
                  "trip_id": "trip-1",
                  "driver_id": "drv-1",
                  "user_id": "u-1",
                  "event": "END",
                  "duration_seconds": 620,
                  "timestamp": "hier a 15h"
                }
                """;

        ValidationOutcome<TripEvent> outcome = TripEventValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<TripEvent> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("missing_field:distance_km", invalid.reason());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            "not json at all",
            "{\"trip_id\": \"trip-1",
            "null",
            "42",
            "[]",
            "{}",
            "{\"event\": null, \"distance_km\": null}",
            "{\"event\": \"END\", \"distance_km\": {}, \"duration_seconds\": []}"
    })
    void neverThrowsOnAnyInput(String raw) {
        assertDoesNotThrow(() -> TripEventValidation.VALIDATOR.validate(raw));
    }
}

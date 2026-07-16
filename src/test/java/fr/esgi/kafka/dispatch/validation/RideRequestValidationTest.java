package fr.esgi.kafka.dispatch.validation;

import fr.esgi.kafka.dispatch.common.ValidationOutcome;
import fr.esgi.kafka.dispatch.model.RideRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Verifie RideRequestValidation.VALIDATOR : chaque test correspond a un
 * critere d'acceptation de l'issue #2 (parse deux phases + chaine de
 * regles metier a court-circuit, cf. ADR 0004).
 */
class RideRequestValidationTest {

    @Test
    void truncatedJsonIsRejectedAsMalformedJson() {
        String raw = "{\"driver_id\": \"drv";

        ValidationOutcome<RideRequest> outcome = RideRequestValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<RideRequest> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("malformed_json", invalid.reason());
        assertEquals(raw, invalid.raw());
    }

    @Test
    void emptyMessageIsRejectedAsMalformedJson() {
        String raw = "";

        ValidationOutcome<RideRequest> outcome = RideRequestValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<RideRequest> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("malformed_json", invalid.reason());
    }

    @Test
    void wellFormedJsonWithWrongFieldTypeIsRejectedWithFieldSpecificReason() {
        String raw = """
                {
                  "request_id": "req-1",
                  "user_id": "u-1",
                  "pickup": {"lat": "douze", "lon": 2.35, "zone": "Z-BASTILLE"},
                  "dropoff": {"lat": 48.85, "lon": 2.36, "zone": "Z-NATION"},
                  "timestamp": "2026-07-04T14:03:21.512Z"
                }
                """;

        ValidationOutcome<RideRequest> outcome = RideRequestValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<RideRequest> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("wrong_type:pickup.lat", invalid.reason());
    }

    @Test
    void wellFormedRideRequestIsValid() {
        String raw = """
                {
                  "request_id": "req-398d1ca6",
                  "user_id": "u-0278",
                  "pickup": {"lat": 48.854972, "lon": 2.375735, "zone": "Z-BASTILLE"},
                  "dropoff": {"lat": 48.848057, "lon": 2.363855, "zone": "Z-NATION"},
                  "timestamp": "2026-07-04T14:03:21.512Z"
                }
                """;

        ValidationOutcome<RideRequest> outcome = RideRequestValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Valid<RideRequest> valid =
                assertInstanceOf(ValidationOutcome.Valid.class, outcome);
        assertEquals("req-398d1ca6", valid.value().requestId());
        assertEquals("Z-BASTILLE", valid.value().pickup().zone());
        assertEquals(raw, valid.raw());
    }

    @Test
    void missingRequestIdIsRejectedWithFieldSpecificReason() {
        String raw = """
                {
                  "user_id": "u-0278",
                  "pickup": {"lat": 48.854972, "lon": 2.375735, "zone": "Z-BASTILLE"},
                  "dropoff": {"lat": 48.848057, "lon": 2.363855, "zone": "Z-NATION"},
                  "timestamp": "2026-07-04T14:03:21.512Z"
                }
                """;

        ValidationOutcome<RideRequest> outcome = RideRequestValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<RideRequest> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("missing_field:request_id", invalid.reason());
    }

    @Test
    void explicitNullOnRequiredNestedFieldIsRejectedWithFieldSpecificReason() {
        String raw = """
                {
                  "request_id": "req-1",
                  "user_id": "u-1",
                  "pickup": {"lat": 48.854972, "lon": 2.375735, "zone": null},
                  "dropoff": {"lat": 48.848057, "lon": 2.363855, "zone": "Z-NATION"},
                  "timestamp": "2026-07-04T14:03:21.512Z"
                }
                """;

        ValidationOutcome<RideRequest> outcome = RideRequestValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<RideRequest> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("missing_field:pickup.zone", invalid.reason());
    }

    @Test
    void unrecognizedZoneIsRejected() {
        String raw = """
                {
                  "request_id": "req-1",
                  "user_id": "u-1",
                  "pickup": {"lat": 48.854972, "lon": 2.375735, "zone": "Z-INCONNUE"},
                  "dropoff": {"lat": 48.848057, "lon": 2.363855, "zone": "Z-NATION"},
                  "timestamp": "2026-07-04T14:03:21.512Z"
                }
                """;

        ValidationOutcome<RideRequest> outcome = RideRequestValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<RideRequest> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("unknown_zone:pickup", invalid.reason());
    }

    @Test
    void unrecognizedZoneOnDropoffIsRejected() {
        String raw = """
                {
                  "request_id": "req-1",
                  "user_id": "u-1",
                  "pickup": {"lat": 48.854972, "lon": 2.375735, "zone": "Z-BASTILLE"},
                  "dropoff": {"lat": 48.848057, "lon": 2.363855, "zone": "Z-INCONNUE"},
                  "timestamp": "2026-07-04T14:03:21.512Z"
                }
                """;

        ValidationOutcome<RideRequest> outcome = RideRequestValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<RideRequest> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("unknown_zone:dropoff", invalid.reason());
    }

    @Test
    void gpsOutOfParisBoundsIsRejected() {
        String raw = """
                {
                  "request_id": "req-1",
                  "user_id": "u-1",
                  "pickup": {"lat": 91.2, "lon": 2.375735, "zone": "Z-BASTILLE"},
                  "dropoff": {"lat": 48.848057, "lon": 2.363855, "zone": "Z-NATION"},
                  "timestamp": "2026-07-04T14:03:21.512Z"
                }
                """;

        ValidationOutcome<RideRequest> outcome = RideRequestValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<RideRequest> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("gps_out_of_bounds:pickup", invalid.reason());
    }

    @Test
    void gpsOutOfParisBoundsOnDropoffIsRejected() {
        String raw = """
                {
                  "request_id": "req-1",
                  "user_id": "u-1",
                  "pickup": {"lat": 48.854972, "lon": 2.375735, "zone": "Z-BASTILLE"},
                  "dropoff": {"lat": 48.848057, "lon": 9.99, "zone": "Z-NATION"},
                  "timestamp": "2026-07-04T14:03:21.512Z"
                }
                """;

        ValidationOutcome<RideRequest> outcome = RideRequestValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<RideRequest> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("gps_out_of_bounds:dropoff", invalid.reason());
    }

    @Test
    void unparseableTimestampIsRejected() {
        String raw = """
                {
                  "request_id": "req-1",
                  "user_id": "u-1",
                  "pickup": {"lat": 48.854972, "lon": 2.375735, "zone": "Z-BASTILLE"},
                  "dropoff": {"lat": 48.848057, "lon": 2.363855, "zone": "Z-NATION"},
                  "timestamp": "hier a 15h"
                }
                """;

        ValidationOutcome<RideRequest> outcome = RideRequestValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<RideRequest> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("unparseable_timestamp", invalid.reason());
    }

    @Test
    void messageFailingMultipleChecksReportsOnlyTheFirstInFixedOrder() {
        // Zone inconnue (plus tard dans l'ordre) ET timestamp illisible
        // (encore plus tard) : seule la zone inconnue doit remonter.
        String raw = """
                {
                  "request_id": "req-1",
                  "user_id": "u-1",
                  "pickup": {"lat": 48.854972, "lon": 2.375735, "zone": "Z-INCONNUE"},
                  "dropoff": {"lat": 48.848057, "lon": 2.363855, "zone": "Z-NATION"},
                  "timestamp": "hier a 15h"
                }
                """;

        ValidationOutcome<RideRequest> outcome = RideRequestValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<RideRequest> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("unknown_zone:pickup", invalid.reason());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            "not json at all",
            "{\"driver_id\": \"drv",
            "null",
            "42",
            "[]",
            "{}",
            "{\"pickup\": null, \"dropoff\": null}",
            "{\"pickup\": {\"lat\": {}, \"lon\": [], \"zone\": 12}}"
    })
    void neverThrowsOnAnyInput(String raw) {
        assertDoesNotThrow(() -> RideRequestValidation.VALIDATOR.validate(raw));
    }
}

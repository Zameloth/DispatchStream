package fr.esgi.kafka.dispatch.validation;

import fr.esgi.kafka.dispatch.common.ValidationOutcome;
import fr.esgi.kafka.dispatch.model.DriverLocation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Verifie DriverLocationValidation.VALIDATOR : chaque test correspond a un
 * critere d'acceptation de l'issue #3 (parse deux phases + chaine de
 * regles metier a court-circuit, cf. ADR 0004), meme convention que
 * RideRequestValidationTest.
 */
class DriverLocationValidationTest {

    @Test
    void truncatedJsonIsRejectedAsMalformedJson() {
        String raw = "{\"driver_id\": \"drv";

        ValidationOutcome<DriverLocation> outcome = DriverLocationValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<DriverLocation> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("malformed_json", invalid.reason());
        assertEquals(raw, invalid.raw());
    }

    @Test
    void emptyMessageIsRejectedAsMalformedJson() {
        String raw = "";

        ValidationOutcome<DriverLocation> outcome = DriverLocationValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<DriverLocation> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("malformed_json", invalid.reason());
    }

    @Test
    void wellFormedJsonWithWrongFieldTypeIsRejectedWithFieldSpecificReason() {
        String raw = """
                {
                  "driver_id": "drv-42",
                  "lat": "quarante-huit",
                  "lon": 2.35,
                  "zone": "Z-BASTILLE",
                  "status": "AVAILABLE",
                  "timestamp": "2026-07-04T14:03:21.512Z"
                }
                """;

        ValidationOutcome<DriverLocation> outcome = DriverLocationValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<DriverLocation> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("wrong_type:lat", invalid.reason());
    }

    @Test
    void wellFormedDriverLocationIsValid() {
        String raw = """
                {
                  "driver_id": "drv-42",
                  "lat": 48.854972,
                  "lon": 2.375735,
                  "zone": "Z-BASTILLE",
                  "status": "AVAILABLE",
                  "timestamp": "2026-07-04T14:03:21.512Z"
                }
                """;

        ValidationOutcome<DriverLocation> outcome = DriverLocationValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Valid<DriverLocation> valid =
                assertInstanceOf(ValidationOutcome.Valid.class, outcome);
        assertEquals("drv-42", valid.value().driverId());
        assertEquals("Z-BASTILLE", valid.value().zone());
        assertEquals(raw, valid.raw());
    }

    @Test
    void missingDriverIdIsRejectedWithFieldSpecificReason() {
        String raw = """
                {
                  "lat": 48.854972,
                  "lon": 2.375735,
                  "zone": "Z-BASTILLE",
                  "status": "AVAILABLE",
                  "timestamp": "2026-07-04T14:03:21.512Z"
                }
                """;

        ValidationOutcome<DriverLocation> outcome = DriverLocationValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<DriverLocation> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("missing_field:driver_id", invalid.reason());
    }

    @Test
    void unrecognizedZoneIsRejected() {
        String raw = """
                {
                  "driver_id": "drv-42",
                  "lat": 48.854972,
                  "lon": 2.375735,
                  "zone": "Z-INCONNUE",
                  "status": "AVAILABLE",
                  "timestamp": "2026-07-04T14:03:21.512Z"
                }
                """;

        ValidationOutcome<DriverLocation> outcome = DriverLocationValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<DriverLocation> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("unknown_zone", invalid.reason());
    }

    @Test
    void unrecognizedStatusIsRejected() {
        String raw = """
                {
                  "driver_id": "drv-42",
                  "lat": 48.854972,
                  "lon": 2.375735,
                  "zone": "Z-BASTILLE",
                  "status": "AVAILABLEE",
                  "timestamp": "2026-07-04T14:03:21.512Z"
                }
                """;

        ValidationOutcome<DriverLocation> outcome = DriverLocationValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<DriverLocation> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("unknown_status", invalid.reason());
    }

    @Test
    void gpsOutOfParisBoundsIsRejected() {
        String raw = """
                {
                  "driver_id": "drv-42",
                  "lat": 91.2,
                  "lon": 2.375735,
                  "zone": "Z-BASTILLE",
                  "status": "AVAILABLE",
                  "timestamp": "2026-07-04T14:03:21.512Z"
                }
                """;

        ValidationOutcome<DriverLocation> outcome = DriverLocationValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<DriverLocation> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("gps_out_of_bounds", invalid.reason());
    }

    @Test
    void unparseableTimestampIsRejected() {
        String raw = """
                {
                  "driver_id": "drv-42",
                  "lat": 48.854972,
                  "lon": 2.375735,
                  "zone": "Z-BASTILLE",
                  "status": "AVAILABLE",
                  "timestamp": "hier a 15h"
                }
                """;

        ValidationOutcome<DriverLocation> outcome = DriverLocationValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<DriverLocation> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("unparseable_timestamp", invalid.reason());
    }

    @Test
    void messageFailingMultipleChecksReportsOnlyTheFirstInFixedOrder() {
        // Zone inconnue (plus tot dans l'ordre) ET timestamp illisible
        // (plus tard) : seule la zone inconnue doit remonter.
        String raw = """
                {
                  "driver_id": "drv-42",
                  "lat": 48.854972,
                  "lon": 2.375735,
                  "zone": "Z-INCONNUE",
                  "status": "AVAILABLE",
                  "timestamp": "hier a 15h"
                }
                """;

        ValidationOutcome<DriverLocation> outcome = DriverLocationValidation.VALIDATOR.validate(raw);

        ValidationOutcome.Invalid<DriverLocation> invalid =
                assertInstanceOf(ValidationOutcome.Invalid.class, outcome);
        assertEquals("unknown_zone", invalid.reason());
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
            "{\"lat\": null, \"lon\": null}",
            "{\"lat\": {}, \"lon\": [], \"zone\": 12}"
    })
    void neverThrowsOnAnyInput(String raw) {
        assertDoesNotThrow(() -> DriverLocationValidation.VALIDATOR.validate(raw));
    }
}

package fr.esgi.kafka.dispatch;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.esgi.kafka.dispatch.model.GhostTripAlert;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Teste le branchement de la detection de courses fantomes (DISP-5,
 * issue #9) dans TopologyProducer.buildTopology() : sur les END du flux
 * valide (issue #4), vitesse moyenne = distance_km / (duration_seconds/3600)
 * ; alerte si vitesse > 130 km/h ou duree > 10800s (cf. CONTEXT.md "Ghost
 * trip"). Meme convention que TripEventTopologyTest (TopologyTestDriver,
 * sans broker).
 */
class GhostTripTopologyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> tripEventsIn;
    private TestOutputTopic<String, String> alertsOut;

    @BeforeEach
    void setUp() {
        Topology topology = new TopologyProducer().buildTopology();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ghost-trip-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        driver = new TopologyTestDriver(topology, props);
        tripEventsIn = driver.createInputTopic(
                Topics.TRIP_EVENTS, Serdes.String().serializer(), Serdes.String().serializer());
        alertsOut = driver.createOutputTopic(
                Topics.ALERTS_TRIPS, Serdes.String().deserializer(), Serdes.String().deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    private void end(String tripId, double distanceKm, int durationSeconds) {
        String raw = """
                {
                  "trip_id": "%s",
                  "driver_id": "drv-1",
                  "user_id": "u-1",
                  "event": "END",
                  "distance_km": %s,
                  "duration_seconds": %d,
                  "timestamp": "2026-07-16T10:12:00Z"
                }
                """.formatted(tripId, distanceKm, durationSeconds);
        tripEventsIn.pipeInput(tripId, raw);
    }

    private void start(String tripId) {
        String raw = """
                {
                  "trip_id": "%s",
                  "driver_id": "drv-1",
                  "user_id": "u-1",
                  "event": "START",
                  "timestamp": "2026-07-16T10:00:00Z"
                }
                """.formatted(tripId);
        tripEventsIn.pipeInput(tripId, raw);
    }

    @Test
    void startEventNeverTriggersAnAlert() {
        start("trip-1");

        assertTrue(alertsOut.isEmpty());
    }

    @Test
    void endWithAverageSpeedAbove130KmhIsFlaggedAsSpeedBased() throws Exception {
        // 100 km en 1200 s (20 min) = 300 km/h.
        end("trip-1", 100.0, 1200);

        GhostTripAlert alert = MAPPER.readValue(alertsOut.readValue(), GhostTripAlert.class);
        assertEquals("trip-1", alert.tripId());
        assertEquals("excessive_speed", alert.reason());
    }

    @Test
    void endWithDurationAbove3HoursIsFlaggedAsDurationBased() throws Exception {
        // 20 km en 10801 s (~3h) = vitesse moyenne normale, seule la duree deborde.
        end("trip-1", 20.0, 10_801);

        GhostTripAlert alert = MAPPER.readValue(alertsOut.readValue(), GhostTripAlert.class);
        assertEquals("trip-1", alert.tripId());
        assertEquals("excessive_duration", alert.reason());
    }

    @Test
    void normalTripsInTheFifteenToFortyKmhRangeNeverTriggerAFalsePositive() {
        end("trip-1", 5.0, 1200);  // 15 km/h
        end("trip-2", 10.0, 1440); // 25 km/h
        end("trip-3", 15.0, 1350); // 40 km/h

        assertTrue(alertsOut.isEmpty());
    }

    @Test
    void alertIsPublishedToTheAlertsTripsTopic() {
        end("trip-1", 900.0, 1200);

        assertTrue(!alertsOut.isEmpty());
    }
}

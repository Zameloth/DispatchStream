package fr.esgi.kafka.dispatch;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.esgi.kafka.dispatch.common.DlqEnvelope;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Teste le branchement de la validation TripEvent dans
 * TopologyProducer.buildTopology() (issue #4), via TopologyTestDriver,
 * sans broker, meme convention que RideRequestTopologyTest (issue #2).
 */
class TripEventTopologyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> tripEventsIn;
    private TestOutputTopic<String, String> dlqOut;

    @BeforeEach
    void setUp() {
        Topology topology = new TopologyProducer().buildTopology();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "trip-event-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        driver = new TopologyTestDriver(topology, props);
        tripEventsIn = driver.createInputTopic(
                Topics.TRIP_EVENTS, Serdes.String().serializer(), Serdes.String().serializer());
        dlqOut = driver.createOutputTopic(
                Topics.DLQ, Serdes.String().deserializer(), Serdes.String().deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void malformedTripEventIsRoutedToDlqWithEnvelope() throws Exception {
        String raw = "{\"trip_id\": \"trip-1";

        tripEventsIn.pipeInput("trip-1", raw);

        assertFalse(dlqOut.isEmpty());
        DlqEnvelope envelope = MAPPER.readValue(dlqOut.readValue(), DlqEnvelope.class);
        assertEquals(Topics.TRIP_EVENTS, envelope.sourceTopic());
        assertEquals("trip-1", envelope.key());
        assertEquals(raw, envelope.rawValue());
        assertEquals("malformed_json", envelope.reason());
    }

    @Test
    void endTripEventMissingDistanceKmIsRoutedToDlqWithSpecificReason() throws Exception {
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

        tripEventsIn.pipeInput("trip-1", raw);

        DlqEnvelope envelope = MAPPER.readValue(dlqOut.readValue(), DlqEnvelope.class);
        assertEquals("missing_field:distance_km", envelope.reason());
        assertEquals(raw, envelope.rawValue());
    }

    @Test
    void wellFormedStartTripEventDoesNotReachTheDlq() {
        String raw = """
                {
                  "trip_id": "trip-1",
                  "driver_id": "drv-1",
                  "user_id": "u-1",
                  "event": "START",
                  "timestamp": "2026-07-16T10:00:00Z"
                }
                """;

        tripEventsIn.pipeInput("trip-1", raw);

        assertTrue(dlqOut.isEmpty());
    }

    @Test
    void wellFormedEndTripEventDoesNotReachTheDlq() {
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

        tripEventsIn.pipeInput("trip-1", raw);

        assertTrue(dlqOut.isEmpty());
    }

    @Test
    void orphanEndTripEventDoesNotReachTheDlq() {
        // Aucun START prealable pour trip-orphan dans ce test : la
        // correlation START/END est hors-perimetre (cf. ADR 0003), un END
        // bien forme doit passer meme sans START correspondant.
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

        tripEventsIn.pipeInput("trip-orphan", raw);

        assertTrue(dlqOut.isEmpty());
    }
}

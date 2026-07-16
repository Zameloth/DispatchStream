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

import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Teste le branchement de la validation RideRequest dans
 * TopologyProducer.buildTopology() (issue #2), via TopologyTestDriver,
 * sans broker, meme convention que DispatchTopologyTest.
 */
class RideRequestTopologyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> rideRequestsIn;
    private TestOutputTopic<String, String> dlqOut;

    @BeforeEach
    void setUp() {
        Topology topology = new TopologyProducer().buildTopology();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ride-request-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        driver = new TopologyTestDriver(topology, props);
        rideRequestsIn = driver.createInputTopic(
                Topics.RIDE_REQUESTS, Serdes.String().serializer(), Serdes.String().serializer());
        dlqOut = driver.createOutputTopic(
                Topics.DLQ, Serdes.String().deserializer(), Serdes.String().deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void malformedRideRequestIsRoutedToDlqWithEnvelope() throws Exception {
        String raw = "{\"driver_id\": \"drv";

        rideRequestsIn.pipeInput("u-0278", raw);

        assertFalse(dlqOut.isEmpty());
        DlqEnvelope envelope = MAPPER.readValue(dlqOut.readValue(), DlqEnvelope.class);
        assertEquals(Topics.RIDE_REQUESTS, envelope.sourceTopic());
        assertEquals("u-0278", envelope.key());
        assertEquals(raw, envelope.rawValue());
        assertEquals("malformed_json", envelope.reason());
    }

    @Test
    void wrongTypeFieldIsRoutedToDlqWithFieldSpecificReason() throws Exception {
        String raw = """
                {
                  "request_id": "req-1",
                  "user_id": "u-1",
                  "pickup": {"lat": "douze", "lon": 2.35, "zone": "Z-BASTILLE"},
                  "dropoff": {"lat": 48.85, "lon": 2.36, "zone": "Z-NATION"},
                  "timestamp": "2026-07-04T14:03:21.512Z"
                }
                """;

        rideRequestsIn.pipeInput("u-1", raw);

        DlqEnvelope envelope = MAPPER.readValue(dlqOut.readValue(), DlqEnvelope.class);
        assertEquals("wrong_type:pickup.lat", envelope.reason());
        assertEquals(raw, envelope.rawValue());
    }

    @Test
    void gpsOutOfBoundsIsRoutedToDlq() throws Exception {
        String raw = """
                {
                  "request_id": "req-1",
                  "user_id": "u-1",
                  "pickup": {"lat": 91.2, "lon": 2.375735, "zone": "Z-BASTILLE"},
                  "dropoff": {"lat": 48.848057, "lon": 2.363855, "zone": "Z-NATION"},
                  "timestamp": "2026-07-04T14:03:21.512Z"
                }
                """;

        rideRequestsIn.pipeInput("u-1", raw);

        DlqEnvelope envelope = MAPPER.readValue(dlqOut.readValue(), DlqEnvelope.class);
        assertEquals("gps_out_of_bounds:pickup", envelope.reason());
    }

    @Test
    void wellFormedRideRequestDoesNotReachTheDlq() {
        String raw = """
                {
                  "request_id": "req-398d1ca6",
                  "user_id": "u-0278",
                  "pickup": {"lat": 48.854972, "lon": 2.375735, "zone": "Z-BASTILLE"},
                  "dropoff": {"lat": 48.848057, "lon": 2.363855, "zone": "Z-NATION"},
                  "timestamp": "2026-07-04T14:03:21.512Z"
                }
                """;

        // Timestamp du record aligne sur le timestamp metier : ce test
        // porte sur la validation stateless, pas sur la detection de
        // retard (cf. Timestamps.isLate, ADR 0007).
        rideRequestsIn.pipeInput("u-0278", raw, Instant.parse("2026-07-04T14:03:21.512Z"));

        assertTrue(dlqOut.isEmpty());
    }
}

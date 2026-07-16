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
 * Teste le branchement de la validation DriverLocation dans
 * TopologyProducer.buildTopology() (issue #3), via TopologyTestDriver,
 * sans broker, meme convention que RideRequestTopologyTest.
 */
class DriverLocationTopologyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> driverLocationsIn;
    private TestOutputTopic<String, String> dlqOut;

    @BeforeEach
    void setUp() {
        Topology topology = new TopologyProducer().buildTopology();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "driver-location-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        driver = new TopologyTestDriver(topology, props);
        driverLocationsIn = driver.createInputTopic(
                Topics.DRIVER_LOCATIONS, Serdes.String().serializer(), Serdes.String().serializer());
        dlqOut = driver.createOutputTopic(
                Topics.DLQ, Serdes.String().deserializer(), Serdes.String().deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void malformedDriverLocationIsRoutedToDlqWithEnvelope() throws Exception {
        String raw = "{\"driver_id\": \"drv";

        driverLocationsIn.pipeInput("drv-42", raw);

        assertFalse(dlqOut.isEmpty());
        DlqEnvelope envelope = MAPPER.readValue(dlqOut.readValue(), DlqEnvelope.class);
        assertEquals(Topics.DRIVER_LOCATIONS, envelope.sourceTopic());
        assertEquals("drv-42", envelope.key());
        assertEquals(raw, envelope.rawValue());
        assertEquals("malformed_json", envelope.reason());
    }

    @Test
    void wrongTypeFieldIsRoutedToDlqWithFieldSpecificReason() throws Exception {
        String raw = """
                {
                  "driver_id": "drv-42",
                  "lat": "quarante-huit",
                  "lon": 2.375735,
                  "zone": "Z-BASTILLE",
                  "status": "AVAILABLE",
                  "timestamp": "2026-07-04T14:03:21.512Z"
                }
                """;

        driverLocationsIn.pipeInput("drv-42", raw);

        DlqEnvelope envelope = MAPPER.readValue(dlqOut.readValue(), DlqEnvelope.class);
        assertEquals("wrong_type:lat", envelope.reason());
        assertEquals(raw, envelope.rawValue());
    }

    @Test
    void gpsOutOfBoundsIsRoutedToDlq() throws Exception {
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

        driverLocationsIn.pipeInput("drv-42", raw);

        DlqEnvelope envelope = MAPPER.readValue(dlqOut.readValue(), DlqEnvelope.class);
        assertEquals("gps_out_of_bounds", envelope.reason());
    }

    @Test
    void wellFormedDriverLocationDoesNotReachTheDlq() {
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

        // Timestamp du record aligne sur le timestamp metier : ce test
        // porte sur la validation stateless, pas sur la detection de
        // retard (cf. Timestamps.isLate, ADR 0007).
        driverLocationsIn.pipeInput("drv-42", raw, Instant.parse("2026-07-04T14:03:21.512Z"));

        assertTrue(dlqOut.isEmpty());
    }
}

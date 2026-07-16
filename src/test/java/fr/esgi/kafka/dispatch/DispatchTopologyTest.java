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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Squelette de tests unitaires de topologie (bonus "tests").
 * Outil : TopologyTestDriver (ajoutez kafka-streams-test-utils en scope
 * test dans le pom, version alignee sur le client Kafka du BOM Quarkus).
 * On teste la Topology nue (new TopologyProducer().buildTopology()),
 * pas besoin de @QuarkusTest ni de broker.
 */
class DispatchTopologyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TopologyTestDriver driver;

    @BeforeEach
    void setUp() {
        Topology topology = new TopologyProducer().buildTopology();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dispatch-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        driver = new TopologyTestDriver(topology, props);
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void pingHorsBornesDoitPartirEnDlq() throws Exception {
        TestInputTopic<String, String> driverLocationsIn = driver.createInputTopic(
                Topics.DRIVER_LOCATIONS, Serdes.String().serializer(), Serdes.String().serializer());
        TestOutputTopic<String, String> dlqOut = driver.createOutputTopic(
                Topics.DLQ, Serdes.String().deserializer(), Serdes.String().deserializer());

        // Ping GPS hors des bornes de Paris (lat 48.7-49.0 / lon 2.2-2.5).
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

        assertFalse(dlqOut.isEmpty());
        DlqEnvelope envelope = MAPPER.readValue(dlqOut.readValue(), DlqEnvelope.class);
        assertEquals(Topics.DRIVER_LOCATIONS, envelope.sourceTopic());
        assertEquals("gps_out_of_bounds", envelope.reason());
        assertEquals(raw, envelope.rawValue());
    }

    @Test
    @Disabled("A implementer : une course a 900 km/h doit lever une alerte")
    void courseAberranteDeclencheUneAlerte() {
    }
}

package fr.esgi.kafka.dispatch.stateful;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.esgi.kafka.dispatch.Topics;
import fr.esgi.kafka.dispatch.TopologyProducer;
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

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Teste la deuxieme etape de validation, stateful, pour DriverLocation
 * (identite = (driver_id, timestamp), ADR 0007) : late, duplicate (meme
 * identite, meme lat/lon), conflicting (meme identite, lat/lon different
 * -> "conflicting location for driver at timestamp", cf. CONTEXT.md).
 * Meme convention que DriverLocationTopologyTest (TopologyTestDriver,
 * sans broker), qui couvre la premiere etape (stateless, issue #3).
 */
class DriverLocationStatefulValidationTopologyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> driverLocationsIn;
    private TestOutputTopic<String, String> dlqOut;

    @BeforeEach
    void setUp() {
        Topology topology = new TopologyProducer().buildTopology();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "driver-location-stateful-validation-topology-test");
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

    private static String driverLocation(String driverId, double lat, double lon, String zone, Instant timestamp) {
        return """
                {
                  "driver_id": "%s",
                  "lat": %s,
                  "lon": %s,
                  "zone": "%s",
                  "status": "AVAILABLE",
                  "timestamp": "%s"
                }
                """.formatted(driverId, lat, lon, zone, timestamp);
    }

    @Test
    void lateDriverLocationIsRoutedToDlqWithReasonLate() throws Exception {
        Instant eventTime = Instant.parse("2026-07-16T10:00:00Z");
        Instant arrival = eventTime.plus(Duration.ofMinutes(10));

        driverLocationsIn.pipeInput(
                "drv-1", driverLocation("drv-1", 48.854972, 2.375735, "Z-BASTILLE", eventTime), arrival);

        DlqEnvelope envelope = MAPPER.readValue(dlqOut.readValue(), DlqEnvelope.class);
        assertEquals("late", envelope.reason());
    }

    @Test
    void duplicatePingWithSameIdentityAndSameLatLonIsRoutedToDlqWithReasonDuplicate() throws Exception {
        Instant eventTs = Instant.parse("2026-07-16T10:00:00Z");
        String raw = driverLocation("drv-1", 48.854972, 2.375735, "Z-BASTILLE", eventTs);
        driverLocationsIn.pipeInput("drv-1", raw, eventTs);
        assertTrue(dlqOut.isEmpty());

        // Meme identite (driver_id, timestamp) et meme contenu, rejouee
        // (retry producteur par exemple) dans la fenetre de 6 min.
        driverLocationsIn.pipeInput("drv-1", raw, eventTs.plusSeconds(30));

        DlqEnvelope envelope = MAPPER.readValue(dlqOut.readValue(), DlqEnvelope.class);
        assertEquals("duplicate", envelope.reason());
        assertEquals(raw, envelope.rawValue());
    }

    @Test
    void conflictingPingWithSameIdentityButDifferentLatLonIsRoutedToDlqWithReason() throws Exception {
        Instant eventTs = Instant.parse("2026-07-16T10:00:00Z");
        driverLocationsIn.pipeInput(
                "drv-1", driverLocation("drv-1", 48.854972, 2.375735, "Z-BASTILLE", eventTs), eventTs);
        assertTrue(dlqOut.isEmpty());

        // Meme identite (driver_id, timestamp) mais lat/lon different :
        // contradiction de donnees, pas une repetition du meme fait
        // (cf. CONTEXT.md "Conflicting event").
        String conflicting = driverLocation("drv-1", 48.86, 2.35, "Z-BASTILLE", eventTs);
        driverLocationsIn.pipeInput("drv-1", conflicting, eventTs.plusSeconds(30));

        DlqEnvelope envelope = MAPPER.readValue(dlqOut.readValue(), DlqEnvelope.class);
        assertEquals("conflicting location for driver at timestamp", envelope.reason());
        assertEquals(conflicting, envelope.rawValue());
    }

    @Test
    void identitySeenAgainAfterWindowExpiryIsTreatedAsFreshOccurrence() {
        // L'identite (driver_id, timestamp) inclut le timestamp metier
        // lui-meme : pour qu'une meme identite reapparaisse hors fenetre
        // de dedup sans etre elle-meme rejetee "late", la premiere
        // occurrence doit etre arrivee en avance sur son propre timestamp
        // (cf. Timestamps.isLate, qui n'invalide que les arrivees tardives).
        Instant eventTs = Instant.parse("2026-07-16T10:00:00Z");
        Instant firstArrival = eventTs.minus(Duration.ofMinutes(5));
        driverLocationsIn.pipeInput(
                "drv-1", driverLocation("drv-1", 48.854972, 2.375735, "Z-BASTILLE", eventTs), firstArrival);
        assertTrue(dlqOut.isEmpty());

        // Meme identite exacte, arrivee plus de 6 min apres la premiere
        // occurrence (firstArrival), mais toujours dans les 6 min de son
        // propre timestamp metier : fenetre de dedup expiree, traite comme
        // une nouvelle occurrence (pas "duplicate").
        Instant secondArrival = eventTs.plus(Duration.ofMinutes(5));
        driverLocationsIn.pipeInput(
                "drv-1", driverLocation("drv-1", 48.854972, 2.375735, "Z-BASTILLE", eventTs), secondArrival);

        assertTrue(dlqOut.isEmpty());
    }
}

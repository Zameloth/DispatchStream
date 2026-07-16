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
 * Teste la deuxieme etape de validation, stateful, pour RideRequest
 * (identite = request_id, ADR 0007) : late (timestamp du record vs
 * timestamp metier), duplicate (meme request_id, meme contenu, dans la
 * fenetre de 6 min), et le choix de perimetre assume par ADR 0007 (pas de
 * categorie "conflicting" pour ce flux). Meme convention que
 * RideRequestTopologyTest (TopologyTestDriver, sans broker), qui couvre
 * la premiere etape (stateless, issue #2).
 */
class RideRequestStatefulValidationTopologyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> rideRequestsIn;
    private TestOutputTopic<String, String> dlqOut;

    @BeforeEach
    void setUp() {
        Topology topology = new TopologyProducer().buildTopology();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ride-request-stateful-validation-topology-test");
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

    private static String rideRequest(String requestId, String userId, String pickupZone, Instant timestamp) {
        return """
                {
                  "request_id": "%s",
                  "user_id": "%s",
                  "pickup": {"lat": 48.854972, "lon": 2.375735, "zone": "%s"},
                  "dropoff": {"lat": 48.848057, "lon": 2.363855, "zone": "Z-NATION"},
                  "timestamp": "%s"
                }
                """.formatted(requestId, userId, pickupZone, timestamp);
    }

    @Test
    void lateRideRequestIsRoutedToDlqWithReasonLate() throws Exception {
        Instant eventTime = Instant.parse("2026-07-16T10:00:00Z");
        Instant arrival = eventTime.plus(Duration.ofMinutes(10));

        rideRequestsIn.pipeInput("u-1", rideRequest("req-1", "u-1", "Z-BASTILLE", eventTime), arrival);

        DlqEnvelope envelope = MAPPER.readValue(dlqOut.readValue(), DlqEnvelope.class);
        assertEquals("late", envelope.reason());
    }

    @Test
    void duplicateRideRequestWithSameContentWithinWindowIsRoutedToDlqWithReasonDuplicate() throws Exception {
        Instant t0 = Instant.parse("2026-07-16T10:00:00Z");
        String raw = rideRequest("req-1", "u-1", "Z-BASTILLE", t0);
        rideRequestsIn.pipeInput("u-1", raw, t0);
        assertTrue(dlqOut.isEmpty());

        rideRequestsIn.pipeInput("u-1", raw, t0.plusSeconds(30));

        DlqEnvelope envelope = MAPPER.readValue(dlqOut.readValue(), DlqEnvelope.class);
        assertEquals("duplicate", envelope.reason());
        assertEquals(raw, envelope.rawValue());
    }

    @Test
    void sameRequestIdWithDifferentContentWithinWindowStaysValid() {
        // Pas de categorie "conflicting" pour ce flux (reservee aux
        // positions chauffeur, cf. CONTEXT.md) : une meme identite dont le
        // contenu differe est laissee valide (choix de perimetre assume
        // par ADR 0007).
        Instant t0 = Instant.parse("2026-07-16T10:00:00Z");
        rideRequestsIn.pipeInput("u-1", rideRequest("req-1", "u-1", "Z-BASTILLE", t0), t0);
        assertTrue(dlqOut.isEmpty());

        Instant t1 = t0.plusSeconds(30);
        rideRequestsIn.pipeInput("u-1", rideRequest("req-1", "u-1", "Z-ODEON", t1), t1);

        assertTrue(dlqOut.isEmpty());
    }

    @Test
    void requestIdSeenAgainAfterWindowExpiryIsTreatedAsFreshOccurrenceNotDuplicate() {
        Instant t0 = Instant.parse("2026-07-16T10:00:00Z");
        rideRequestsIn.pipeInput("u-1", rideRequest("req-1", "u-1", "Z-BASTILLE", t0), t0);
        assertTrue(dlqOut.isEmpty());

        // Meme request_id et meme contenu, mais reapparait plus de 6 min
        // apres la premiere occurrence : la fenetre de dedup est expiree,
        // traite comme une nouvelle occurrence (pas "duplicate").
        Instant t1 = t0.plus(Duration.ofMinutes(7));
        rideRequestsIn.pipeInput("u-1", rideRequest("req-1", "u-1", "Z-BASTILLE", t1), t1);

        assertTrue(dlqOut.isEmpty());
    }
}

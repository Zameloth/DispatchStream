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
 * Teste la deuxieme etape de validation, stateful, pour TripEvent
 * (identite = (trip_id, event), ADR 0007, completant ADR 0003) : late,
 * duplicate (meme identite, meme contenu), et orphan-END (un END sans
 * START correspondant dans la fenetre de 6 min -> "no matching START for
 * trip_id", cf. CONTEXT.md). Meme convention que TripEventTopologyTest
 * (TopologyTestDriver, sans broker), qui couvre la premiere etape
 * (stateless, issue #4) et le cas nominal orphan/well-formed via la
 * topologie complete.
 */
class TripEventStatefulValidationTopologyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> tripEventsIn;
    private TestOutputTopic<String, String> dlqOut;

    @BeforeEach
    void setUp() {
        Topology topology = new TopologyProducer().buildTopology();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "trip-event-stateful-validation-topology-test");
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

    private static String start(String tripId, String driverId, Instant timestamp) {
        return """
                {
                  "trip_id": "%s",
                  "driver_id": "%s",
                  "user_id": "u-1",
                  "event": "START",
                  "timestamp": "%s"
                }
                """.formatted(tripId, driverId, timestamp);
    }

    private static String end(String tripId, String driverId, double distanceKm, int durationSeconds, Instant timestamp) {
        return """
                {
                  "trip_id": "%s",
                  "driver_id": "%s",
                  "user_id": "u-1",
                  "event": "END",
                  "distance_km": %s,
                  "duration_seconds": %d,
                  "timestamp": "%s"
                }
                """.formatted(tripId, driverId, distanceKm, durationSeconds, timestamp);
    }

    @Test
    void lateTripEventIsRoutedToDlqWithReasonLate() throws Exception {
        Instant eventTime = Instant.parse("2026-07-16T10:00:00Z");
        Instant arrival = eventTime.plus(Duration.ofMinutes(10));

        tripEventsIn.pipeInput("trip-1", start("trip-1", "drv-1", eventTime), arrival);

        DlqEnvelope envelope = MAPPER.readValue(dlqOut.readValue(), DlqEnvelope.class);
        assertEquals("late", envelope.reason());
    }

    @Test
    void duplicateStartWithSameContentWithinWindowIsRoutedToDlqWithReasonDuplicate() throws Exception {
        Instant t0 = Instant.parse("2026-07-16T10:00:00Z");
        String raw = start("trip-1", "drv-1", t0);
        tripEventsIn.pipeInput("trip-1", raw, t0);
        assertTrue(dlqOut.isEmpty());

        tripEventsIn.pipeInput("trip-1", raw, t0.plusSeconds(30));

        DlqEnvelope envelope = MAPPER.readValue(dlqOut.readValue(), DlqEnvelope.class);
        assertEquals("duplicate", envelope.reason());
        assertEquals(raw, envelope.rawValue());
    }

    @Test
    void sameTripIdAndEventWithDifferentContentWithinWindowStaysValid() {
        // Pas de categorie "conflicting" pour ce flux (cf.
        // RideRequestStatefulValidationTopologyTest) : une meme identite
        // (trip_id, event) dont le contenu differe (ici driver_id) est
        // laissee valide (choix de perimetre assume par ADR 0007).
        Instant t0 = Instant.parse("2026-07-16T10:00:00Z");
        tripEventsIn.pipeInput("trip-1", start("trip-1", "drv-1", t0), t0);
        assertTrue(dlqOut.isEmpty());

        Instant t1 = t0.plusSeconds(30);
        tripEventsIn.pipeInput("trip-1", start("trip-1", "drv-2", t1), t1);

        assertTrue(dlqOut.isEmpty());
    }

    @Test
    void tripEventIdentitySeenAgainAfterWindowExpiryIsTreatedAsFreshOccurrence() {
        Instant t0 = Instant.parse("2026-07-16T10:00:00Z");
        tripEventsIn.pipeInput("trip-1", start("trip-1", "drv-1", t0), t0);
        assertTrue(dlqOut.isEmpty());

        // Meme identite (trip_id, "START"), mais reapparait plus de 6 min
        // apres la premiere occurrence : fenetre de dedup expiree, traite
        // comme une nouvelle occurrence (pas "duplicate").
        Instant t1 = t0.plus(Duration.ofMinutes(7));
        tripEventsIn.pipeInput("trip-1", start("trip-1", "drv-1", t1), t1);

        assertTrue(dlqOut.isEmpty());
    }

    @Test
    void endWithNoStartAtAllIsRoutedToDlqAsOrphan() throws Exception {
        Instant t0 = Instant.parse("2026-07-16T10:00:00Z");
        String raw = end("trip-orphan", "drv-1", 4.2, 620, t0);

        tripEventsIn.pipeInput("trip-orphan", raw, t0);

        DlqEnvelope envelope = MAPPER.readValue(dlqOut.readValue(), DlqEnvelope.class);
        assertEquals("no matching START for trip_id", envelope.reason());
        assertEquals(raw, envelope.rawValue());
    }

    @Test
    void endWithStartOutsideTheRetentionWindowIsStillRoutedToDlqAsOrphan() throws Exception {
        Instant startTs = Instant.parse("2026-07-16T10:00:00Z");
        tripEventsIn.pipeInput("trip-1", start("trip-1", "drv-1", startTs), startTs);
        assertTrue(dlqOut.isEmpty());

        // START vu il y a plus de 6 min : hors fenetre de retention, ne
        // compte plus pour la correlation (meme borne que le seuil de
        // retard, cf. CONTEXT.md "Orphan END event").
        Instant endTs = startTs.plus(Duration.ofMinutes(7));
        String rawEnd = end("trip-1", "drv-1", 4.2, 620, endTs);
        tripEventsIn.pipeInput("trip-1", rawEnd, endTs);

        DlqEnvelope envelope = MAPPER.readValue(dlqOut.readValue(), DlqEnvelope.class);
        assertEquals("no matching START for trip_id", envelope.reason());
        assertEquals(rawEnd, envelope.rawValue());
    }
}

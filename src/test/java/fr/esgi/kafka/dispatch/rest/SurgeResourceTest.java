package fr.esgi.kafka.dispatch.rest;

import fr.esgi.kafka.dispatch.TopologyProducer;
import fr.esgi.kafka.dispatch.model.SurgeStatus;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Teste SurgeResource (DISP-6 bonus, issue #10) sans broker Kafka reel :
 * la topologie tourne dans un TopologyTestDriver (comme SurgeTopologyTest),
 * dont on prend directement le KeyValueStore pour piloter le chemin
 * "zone connue" de SurgeResource.ratioResponse. Le chemin "zone inconnue"
 * ne touche jamais le champ streams, donc testable sans rien injecter.
 * Le chemin "store pas encore pret" est couvert via un vrai KafkaStreams
 * jamais demarre (etat CREATED), qui fait bien lever InvalidStateStoreException
 * sur .store(...), sans avoir besoin d'un framework de mock.
 */
class SurgeResourceTest {

    private static final Instant T0 = Instant.parse("2026-07-04T14:00:00.00Z");

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> rideRequestsIn;
    private TestInputTopic<String, String> driverLocationsIn;
    private KeyValueStore<String, Double> surgeRatioStore;

    @BeforeEach
    void setUp() {
        Topology topology = new TopologyProducer().buildTopology();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "surge-resource-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        driver = new TopologyTestDriver(topology, props);
        rideRequestsIn = driver.createInputTopic(
                fr.esgi.kafka.dispatch.Topics.RIDE_REQUESTS,
                Serdes.String().serializer(), Serdes.String().serializer());
        driverLocationsIn = driver.createInputTopic(
                fr.esgi.kafka.dispatch.Topics.DRIVER_LOCATIONS,
                Serdes.String().serializer(), Serdes.String().serializer());
        surgeRatioStore = driver.getKeyValueStore(TopologyProducer.SURGE_RATIO_STORE);
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    private void ping(String driverId, String zone, String status, Instant timestamp) {
        String raw = """
                {
                  "driver_id": "%s",
                  "lat": 48.854972,
                  "lon": 2.375735,
                  "zone": "%s",
                  "status": "%s",
                  "timestamp": "%s"
                }
                """.formatted(driverId, zone, status, timestamp);
        driverLocationsIn.pipeInput(driverId, raw, timestamp);
    }

    private void rideRequest(String requestId, String zone, Instant timestamp) {
        String raw = """
                {
                  "request_id": "%s",
                  "user_id": "u-%s",
                  "pickup": {"lat": 48.854972, "lon": 2.375735, "zone": "%s"},
                  "dropoff": {"lat": 48.848057, "lon": 2.363855, "zone": "Z-NATION"},
                  "timestamp": "%s"
                }
                """.formatted(requestId, requestId, zone, timestamp);
        rideRequestsIn.pipeInput(requestId, raw, timestamp);
    }

    @Test
    void unknownZoneReturns404WithoutTouchingTheStore() {
        SurgeResource resource = new SurgeResource();

        Response response = resource.surge("Z-NOT-A-REAL-ZONE");

        assertEquals(404, response.getStatus());
    }

    @Test
    void knownZoneWithNoRecordedDemandReportsAZeroRatioNotSurging() {
        SurgeStatus status = (SurgeStatus) SurgeResource.ratioResponse(surgeRatioStore, "Z-PIGALLE").getEntity();

        assertEquals("Z-PIGALLE", status.zone());
        assertEquals(0.0, status.ratio(), 1e-9);
        assertFalse(status.surging());
    }

    @Test
    void knownZoneCurrentlySurgingReportsTheLiveRatioAboveTwo() {
        ping("drv-1", "Z-BERCY", "AVAILABLE", T0);
        rideRequest("req-1", "Z-BERCY", T0.plusSeconds(1));
        rideRequest("req-2", "Z-BERCY", T0.plusSeconds(2));
        rideRequest("req-3", "Z-BERCY", T0.plusSeconds(3));

        Response response = SurgeResource.ratioResponse(surgeRatioStore, "Z-BERCY");
        SurgeStatus status = (SurgeStatus) response.getEntity();

        assertEquals(200, response.getStatus());
        assertEquals("Z-BERCY", status.zone());
        assertEquals(3.0, status.ratio(), 1e-9);
        assertTrue(status.surging());
    }

    @Test
    void zoneThatDroppedBackBelowThresholdReportsItsCurrentLowerRatioNotTheStaleSurge() {
        // Preuve du choix ADR 0002 (double materialisation) : la zone a
        // surge puis l'offre est revenue, le store non filtre doit
        // refleter l'etat courant, pas le pic passe - contrairement au
        // topic filtre dispatch.surge qui garderait seulement l'evenement
        // du pic.
        ping("drv-1", "Z-ETOILE", "AVAILABLE", T0);
        rideRequest("req-1", "Z-ETOILE", T0.plusSeconds(1));
        rideRequest("req-2", "Z-ETOILE", T0.plusSeconds(2));
        rideRequest("req-3", "Z-ETOILE", T0.plusSeconds(3));

        assertTrue(((SurgeStatus) SurgeResource.ratioResponse(surgeRatioStore, "Z-ETOILE").getEntity()).surging());

        ping("drv-2", "Z-ETOILE", "AVAILABLE", T0.plusSeconds(4));
        ping("drv-3", "Z-ETOILE", "AVAILABLE", T0.plusSeconds(5));
        ping("drv-4", "Z-ETOILE", "AVAILABLE", T0.plusSeconds(6));

        SurgeStatus status = (SurgeStatus) SurgeResource.ratioResponse(surgeRatioStore, "Z-ETOILE").getEntity();
        assertEquals(0.75, status.ratio(), 1e-9, "3 demandes / 4 chauffeurs desormais disponibles");
        assertFalse(status.surging());
    }

    @Test
    void storeNotYetReadyReturns503InsteadOfThrowing() {
        Topology topology = new TopologyProducer().buildTopology();
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "surge-resource-not-started-" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");

        SurgeResource resource = new SurgeResource();
        try (KafkaStreams neverStarted = new KafkaStreams(topology, props)) {
            resource.streams = neverStarted;

            Response response = resource.surge("Z-BERCY");

            assertEquals(503, response.getStatus());
        }
    }
}

package fr.esgi.kafka.dispatch;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.esgi.kafka.dispatch.model.SurgeEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Teste le branchement du surge pricing (DISP-4, issue #8) dans
 * TopologyProducer.buildTopology() : jointure table-table demande/offre
 * aplatie a la cle zone (ADR 0006), ratio non filtre materialise dans
 * SURGE_RATIO_STORE pour toutes les zones (ADR 0002), seuil (> 2)
 * applique uniquement a la publication sur Topics.SURGE.
 */
class SurgeTopologyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Instant T0 = Instant.parse("2026-07-04T14:00:00.00Z");

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> rideRequestsIn;
    private TestInputTopic<String, String> driverLocationsIn;
    private TestOutputTopic<String, String> surgeOut;
    private KeyValueStore<String, Double> surgeRatioStore;

    @BeforeEach
    void setUp() {
        Topology topology = new TopologyProducer().buildTopology();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "surge-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        driver = new TopologyTestDriver(topology, props);
        rideRequestsIn = driver.createInputTopic(
                Topics.RIDE_REQUESTS, Serdes.String().serializer(), Serdes.String().serializer());
        driverLocationsIn = driver.createInputTopic(
                Topics.DRIVER_LOCATIONS, Serdes.String().serializer(), Serdes.String().serializer());
        surgeOut = driver.createOutputTopic(
                Topics.SURGE, Serdes.String().deserializer(), Serdes.String().deserializer());
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

    private SurgeEvent readSurge(String json) {
        try {
            return MAPPER.readValue(json, SurgeEvent.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void ratioAboveTwoPublishesSurgeEventWithTheComputedRatio() {
        ping("drv-1", "Z-BERCY", "AVAILABLE", T0);

        rideRequest("req-1", "Z-BERCY", T0.plusSeconds(1));
        rideRequest("req-2", "Z-BERCY", T0.plusSeconds(2));
        rideRequest("req-3", "Z-BERCY", T0.plusSeconds(3));

        List<KeyValue<String, String>> records = surgeOut.readKeyValuesToList();
        assertTrue(records.size() >= 1, "le franchissement du seuil doit publier au moins un evenement");

        SurgeEvent last = readSurge(records.get(records.size() - 1).value);
        assertEquals("Z-BERCY", last.zone());
        assertEquals(3.0, last.ratio(), 1e-9);
    }

    @Test
    void ratioAtOrBelowTwoStaysSilentOnSurgeTopic() {
        ping("drv-1", "Z-NATION", "AVAILABLE", T0);
        ping("drv-2", "Z-NATION", "AVAILABLE", T0.plusSeconds(1));

        rideRequest("req-1", "Z-NATION", T0.plusSeconds(2));
        rideRequest("req-2", "Z-NATION", T0.plusSeconds(3));
        rideRequest("req-3", "Z-NATION", T0.plusSeconds(4));
        rideRequest("req-4", "Z-NATION", T0.plusSeconds(5));

        assertTrue(surgeOut.isEmpty(), "ratio 4/2=2, pas strictement > 2 : aucune publication attendue");
    }

    @Test
    void unfilteredRatioIsMaterializedForEveryZoneRegardlessOfThreshold() {
        ping("drv-1", "Z-ODEON", "AVAILABLE", T0);
        ping("drv-2", "Z-ODEON", "AVAILABLE", T0.plusSeconds(1));

        rideRequest("req-1", "Z-ODEON", T0.plusSeconds(2));
        rideRequest("req-2", "Z-ODEON", T0.plusSeconds(3));

        assertTrue(surgeOut.isEmpty(), "ratio 2/2=1, pas de publication");
        assertEquals(1.0, surgeRatioStore.get("Z-ODEON"), 1e-9,
                "le ratio non filtre doit malgre tout etre materialise pour interrogation (ADR 0002)");
    }

    @Test
    void zoneWithNoRecordedSupplyUsesOneAsTheDenominatorFloor() {
        rideRequest("req-1", "Z-PIGALLE", T0);

        assertEquals(1.0, surgeRatioStore.get("Z-PIGALLE"), 1e-9,
                "aucune offre jamais vue pour cette zone : demande/max(0,1) = demande");
        assertTrue(surgeOut.isEmpty());

        rideRequest("req-2", "Z-PIGALLE", T0.plusSeconds(1));
        rideRequest("req-3", "Z-PIGALLE", T0.plusSeconds(2));

        List<KeyValue<String, String>> records = surgeOut.readKeyValuesToList();
        SurgeEvent last = readSurge(records.get(records.size() - 1).value);
        assertEquals("Z-PIGALLE", last.zone());
        assertEquals(3.0, last.ratio(), 1e-9);
    }

    @Test
    void supplyDroppingAloneCanTriggerSurgeWithoutNewDemand() {
        // Jointure table-table (ADR 0006) : le ratio doit se recalculer
        // sur une mise a jour de l'offre seule, pas seulement de la demande.
        ping("drv-1", "Z-DEFENSE", "AVAILABLE", T0);
        ping("drv-2", "Z-DEFENSE", "AVAILABLE", T0.plusSeconds(1));

        rideRequest("req-1", "Z-DEFENSE", T0.plusSeconds(2));
        rideRequest("req-2", "Z-DEFENSE", T0.plusSeconds(3));
        rideRequest("req-3", "Z-DEFENSE", T0.plusSeconds(4));

        assertTrue(surgeOut.isEmpty(), "ratio 3/2=1.5, pas de publication tant que l'offre ne bouge pas");

        ping("drv-1", "Z-DEFENSE", "OFFLINE", T0.plusSeconds(5));
        ping("drv-2", "Z-DEFENSE", "OFFLINE", T0.plusSeconds(6));

        List<KeyValue<String, String>> records = surgeOut.readKeyValuesToList();
        assertTrue(records.size() >= 1,
                "l'offre tombant a 0 sans nouvelle demande doit quand meme declencher un surge (3/1=3 > 2)");

        SurgeEvent last = readSurge(records.get(records.size() - 1).value);
        assertEquals("Z-DEFENSE", last.zone());
        assertEquals(3.0, last.ratio(), 1e-9);
    }

    @Test
    void recurringSurgeBercyIncidentProducesASurgeEventOnZBercy() {
        // Simule un incident surge_bercy : peu de chauffeurs disponibles,
        // rafale de demandes (cf. README "le generateur provoque un surge
        // a Bercy environ toutes les 6 min").
        ping("drv-1", "Z-BERCY", "AVAILABLE", T0);

        for (int i = 0; i < 6; i++) {
            rideRequest("req-bercy-" + i, "Z-BERCY", T0.plusSeconds(i + 1));
        }

        List<KeyValue<String, String>> records = surgeOut.readKeyValuesToList();
        assertTrue(records.stream().anyMatch(r -> r.key.equals("Z-BERCY")),
                "un incident surge_bercy doit produire un evenement de surge sur Z-BERCY");
    }
}

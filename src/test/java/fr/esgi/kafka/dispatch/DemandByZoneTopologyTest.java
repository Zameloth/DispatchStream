package fr.esgi.kafka.dispatch;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.esgi.kafka.dispatch.model.DemandByZone;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
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
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Teste le branchement de la demande par zone (DISP-2, issue #6) dans
 * TopologyProducer.buildTopology() : groupement par pickup.zone (pas la
 * cle du record, qui est user_id), fenetres tumbling de 2 min / 6 min de
 * grace, emission continue sans suppress() (ADR 0001). Meme convention
 * que RideRequestTopologyTest (TopologyTestDriver, sans broker).
 */
class DemandByZoneTopologyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Instant T0 = Instant.parse("2026-07-04T14:00:00.00Z");

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> rideRequestsIn;
    private TestOutputTopic<String, String> demandOut;

    @BeforeEach
    void setUp() {
        Topology topology = new TopologyProducer().buildTopology();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "demand-by-zone-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        driver = new TopologyTestDriver(topology, props);
        rideRequestsIn = driver.createInputTopic(
                Topics.RIDE_REQUESTS, Serdes.String().serializer(), Serdes.String().serializer());
        demandOut = driver.createOutputTopic(
                Topics.DEMAND_BY_ZONE, Serdes.String().deserializer(), Serdes.String().deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void groupsByPickupZoneRatherThanTheRecordKey() {
        rideRequestsIn.pipeInput("u-1", rideRequest("req-1", "u-1", "Z-BASTILLE"), T0);
        rideRequestsIn.pipeInput("u-2", rideRequest("req-2", "u-2", "Z-BASTILLE"), T0.plusSeconds(10));

        List<KeyValue<String, String>> records = demandOut.readKeyValuesToList();
        assertEquals(2, records.size());
        for (KeyValue<String, String> record : records) {
            assertEquals("Z-BASTILLE", record.key);
        }
        DemandByZone last = readDemand(records.get(1).value);
        assertEquals("Z-BASTILLE", last.zone());
        assertEquals(2L, last.count());
    }

    @Test
    void emitsContinuouslyWithoutWaitingForTheWindowToClose() throws Exception {
        rideRequestsIn.pipeInput("u-1", rideRequest("req-1", "u-1", "Z-BERCY"), T0);
        rideRequestsIn.pipeInput("u-2", rideRequest("req-2", "u-2", "Z-BERCY"), T0.plusSeconds(30));
        rideRequestsIn.pipeInput("u-3", rideRequest("req-3", "u-3", "Z-BERCY"), T0.plusSeconds(60));

        List<String> counts = demandOut.readValuesToList().stream()
                .map(json -> readDemand(json).count())
                .map(String::valueOf)
                .toList();

        assertEquals(List.of("1", "2", "3"), counts);
    }

    @Test
    void newTumblingWindowResetsTheCount() {
        rideRequestsIn.pipeInput("u-1", rideRequest("req-1", "u-1", "Z-BASTILLE"), T0);
        rideRequestsIn.pipeInput("u-2", rideRequest("req-2", "u-2", "Z-BASTILLE"), T0.plusSeconds(30));
        // Fenetre suivante (tumbling 2 min) : le compte repart de 1.
        rideRequestsIn.pipeInput("u-3", rideRequest("req-3", "u-3", "Z-BASTILLE"), T0.plus(Duration.ofMinutes(3)));

        List<DemandByZone> demands = demandOut.readValuesToList().stream()
                .map(this::readDemand)
                .toList();

        assertEquals(3, demands.size());
        assertEquals(2L, demands.get(1).count());
        assertEquals(1L, demands.get(2).count());
        assertTrue(demands.get(2).windowStart().compareTo(demands.get(0).windowStart()) > 0,
                "la 3e fenetre doit demarrer apres la 1ere");
    }

    @Test
    void recurringBurstIsVisibleAsAnElevatedCountInThatZonesWindow() {
        // Rafale Bercy (~toutes les 6 min dans le trafic genere) : plusieurs
        // demandes groupees dans la meme fenetre de 2 min sur Z-BERCY.
        for (int i = 0; i < 5; i++) {
            rideRequestsIn.pipeInput(
                    "u-bercy-" + i, rideRequest("req-bercy-" + i, "u-bercy-" + i, "Z-BERCY"),
                    T0.plusSeconds(i * 10L));
        }
        rideRequestsIn.pipeInput("u-quiet", rideRequest("req-quiet", "u-quiet", "Z-NATION"), T0.plusSeconds(5));

        List<DemandByZone> demands = demandOut.readValuesToList().stream()
                .map(this::readDemand)
                .toList();

        long bercyPeak = demands.stream()
                .filter(d -> d.zone().equals("Z-BERCY"))
                .mapToLong(DemandByZone::count)
                .max().orElseThrow();
        long nationPeak = demands.stream()
                .filter(d -> d.zone().equals("Z-NATION"))
                .mapToLong(DemandByZone::count)
                .max().orElseThrow();

        assertEquals(5L, bercyPeak);
        assertEquals(1L, nationPeak);
        assertTrue(bercyPeak > nationPeak);
    }

    @Test
    void lateEventWithinTheGracePeriodStillLandsInItsWindow() {
        // Un evenement peut arriver jusqu'a 6 min apres son timestamp
        // (seuil de retard accepte, cf. CONTEXT.md / RideRequestValidation)
        // sans etre rejete : la fenetre doit donc avoir une grace de 6 min
        // pour ne pas le perdre silencieusement (ADR 0001 / issue #6).
        rideRequestsIn.pipeInput("u-1", rideRequest("req-1", "u-1", "Z-ODEON"), T0);
        // Fait avancer le stream-time pres de la limite de grace de la
        // fenetre de u-1 ([T0, T0+2min), close a T0+2min+6min=T0+8min)
        // sans la depasser.
        rideRequestsIn.pipeInput("u-2", rideRequest("req-2", "u-2", "Z-ODEON"), T0.plus(Duration.ofMinutes(7)));
        // Arrive en retard (livre apres u-2) mais son timestamp appartient
        // toujours a la fenetre de u-1, qui n'est pas encore fermee : doit
        // mettre a jour son compte, pas etre ignore.
        rideRequestsIn.pipeInput("u-3", rideRequest("req-3", "u-3", "Z-ODEON"), T0.plusSeconds(90));

        List<DemandByZone> demands = demandOut.readValuesToList().stream()
                .map(this::readDemand)
                .toList();

        DemandByZone firstWindowFinal = demands.get(demands.size() - 1);
        assertEquals(2L, firstWindowFinal.count());
        assertEquals(demands.get(0).windowStart(), firstWindowFinal.windowStart());
    }

    private DemandByZone readDemand(String json) {
        try {
            return MAPPER.readValue(json, DemandByZone.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String rideRequest(String requestId, String userId, String pickupZone) {
        // Le timestamp du corps JSON n'est pas exploite par le fenetrage
        // (qui se base sur le timestamp du record Kafka, fixe explicitement
        // par pipeInput(..., Instant) dans chaque test, comme le fait
        // rejouer.py avec ts_ms en conditions reelles) ; une valeur fixe
        // suffit ici tant qu'elle est parsable (RideRequestValidation).
        return """
                {
                  "request_id": "%s",
                  "user_id": "%s",
                  "pickup": {"lat": 48.854972, "lon": 2.375735, "zone": "%s"},
                  "dropoff": {"lat": 48.848057, "lon": 2.363855, "zone": "Z-NATION"},
                  "timestamp": "2026-07-04T14:00:00.00Z"
                }
                """.formatted(requestId, userId, pickupZone);
    }
}

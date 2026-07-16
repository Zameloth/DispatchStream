package fr.esgi.kafka.dispatch;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.esgi.kafka.dispatch.model.SupplyByZone;
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

import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Teste le branchement de l'offre par zone (issue #7) dans
 * TopologyProducer.buildTopology() : KTable de la derniere position
 * connue par chauffeur, puis re-agregation par zone (cf. README DISP-3).
 */
class SupplyByZoneTopologyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> driverLocationsIn;
    private TestOutputTopic<String, String> supplyOut;

    @BeforeEach
    void setUp() {
        Topology topology = new TopologyProducer().buildTopology();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "supply-by-zone-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        driver = new TopologyTestDriver(topology, props);
        driverLocationsIn = driver.createInputTopic(
                Topics.DRIVER_LOCATIONS, Serdes.String().serializer(), Serdes.String().serializer());
        supplyOut = driver.createOutputTopic(
                Topics.SUPPLY_BY_ZONE, Serdes.String().deserializer(), Serdes.String().deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    private void ping(String driverId, String zone, String status, String timestamp) {
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
        // Timestamp du record aligne sur le timestamp metier : ce test ne
        // porte pas sur la detection de retard (cf. Timestamps.isLate, ADR 0007).
        driverLocationsIn.pipeInput(driverId, raw, Instant.parse(timestamp));
    }

    /**
     * Derniere valeur emise pour chaque zone parmi les enregistrements
     * donnes. readKeyValuesToList() draine le topic de sortie : on ne
     * l'appelle donc qu'une fois par etape et on cherche dans la liste
     * obtenue, plutot que de rappeler cette methode plusieurs fois pour
     * des zones differentes (ce qui perdrait les enregistrements deja
     * draines par le premier appel).
     */
    private long lastSupplyFor(String zone, List<KeyValue<String, String>> records) {
        long last = -1;
        for (KeyValue<String, String> record : records) {
            if (record.key.equals(zone)) {
                try {
                    last = MAPPER.readValue(record.value, SupplyByZone.class).availableDrivers();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return last;
    }

    @Test
    void repeatedPingsFromSameAvailableDriverCountOnlyOnce() {
        ping("drv-1", "Z-BASTILLE", "AVAILABLE", "2026-07-04T14:00:00.000Z");
        ping("drv-1", "Z-BASTILLE", "AVAILABLE", "2026-07-04T14:00:08.000Z");
        ping("drv-1", "Z-BASTILLE", "AVAILABLE", "2026-07-04T14:00:16.000Z");

        assertEquals(1, lastSupplyFor("Z-BASTILLE", supplyOut.readKeyValuesToList()));
    }

    @Test
    void statusChangeAwayFromAvailableRemovesDriverFromZoneCount() {
        ping("drv-1", "Z-BASTILLE", "AVAILABLE", "2026-07-04T14:00:00.000Z");
        ping("drv-1", "Z-BASTILLE", "BUSY", "2026-07-04T14:00:08.000Z");

        List<KeyValue<String, String>> records = supplyOut.readKeyValuesToList();

        assertEquals(0, lastSupplyFor("Z-BASTILLE", records));
    }

    @Test
    void zoneChangeWhileAvailableMovesCountBetweenZonesWithoutDoubleCounting() {
        ping("drv-1", "Z-BASTILLE", "AVAILABLE", "2026-07-04T14:00:00.000Z");
        ping("drv-1", "Z-NATION", "AVAILABLE", "2026-07-04T14:00:08.000Z");

        List<KeyValue<String, String>> records = supplyOut.readKeyValuesToList();

        assertEquals(0, lastSupplyFor("Z-BASTILLE", records));
        assertEquals(1, lastSupplyFor("Z-NATION", records));
    }

    @Test
    void driverGoingOfflineDisappearsFromSupplyEntirely() {
        ping("drv-1", "Z-CHATELET", "AVAILABLE", "2026-07-04T14:00:00.000Z");
        ping("drv-1", "Z-CHATELET", "OFFLINE", "2026-07-04T14:00:08.000Z");

        List<KeyValue<String, String>> records = supplyOut.readKeyValuesToList();

        assertEquals(0, lastSupplyFor("Z-CHATELET", records));
    }

    @Test
    void severalAvailableDriversInSameZoneAreSummed() {
        ping("drv-1", "Z-ODEON", "AVAILABLE", "2026-07-04T14:00:00.000Z");
        ping("drv-2", "Z-ODEON", "AVAILABLE", "2026-07-04T14:00:08.000Z");
        ping("drv-3", "Z-ODEON", "AVAILABLE", "2026-07-04T14:00:16.000Z");

        assertEquals(3, lastSupplyFor("Z-ODEON", supplyOut.readKeyValuesToList()));
    }

    @Test
    void invalidPingIsNotCountedInSupply() {
        // GPS hors bornes Paris (cf. DriverLocationValidation) : rejete en DLQ,
        // ne doit jamais atteindre l'offre par zone.
        String raw = """
                {
                  "driver_id": "drv-1",
                  "lat": 91.2,
                  "lon": 2.375735,
                  "zone": "Z-PIGALLE",
                  "status": "AVAILABLE",
                  "timestamp": "2026-07-04T14:00:00.000Z"
                }
                """;
        driverLocationsIn.pipeInput("drv-1", raw);

        assertTrue(supplyOut.isEmpty());
    }
}

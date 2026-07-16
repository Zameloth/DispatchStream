package fr.esgi.kafka.dispatch;

import fr.esgi.kafka.dispatch.common.DlqEnvelope;
import fr.esgi.kafka.dispatch.common.EventValidator;
import fr.esgi.kafka.dispatch.common.JsonSerdes;
import fr.esgi.kafka.dispatch.common.ValidationOutcome;
import fr.esgi.kafka.dispatch.model.DemandByZone;
import fr.esgi.kafka.dispatch.model.DriverLocation;
import fr.esgi.kafka.dispatch.model.RideRequest;
import fr.esgi.kafka.dispatch.model.SupplyByZone;
import fr.esgi.kafka.dispatch.model.SurgeEvent;
import fr.esgi.kafka.dispatch.model.TripEvent;
import fr.esgi.kafka.dispatch.validation.DriverLocationValidation;
import fr.esgi.kafka.dispatch.validation.RideRequestValidation;
import fr.esgi.kafka.dispatch.validation.TripEventValidation;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;

/**
 * Construisez ici la topologie du projet DISPATCH.
 * Quarkus detecte ce @Produces Topology et demarre Kafka Streams
 * automatiquement (extension quarkus-kafka-streams).
 * Chaque ticket du backlog (README) correspond a un bloc ci-dessous.
 */
@ApplicationScoped
public class TopologyProducer {

    /** Nom du state store queryable du ratio de surge, lu par DISP-6 (bonus, issue #10). */
    public static final String SURGE_RATIO_STORE = "surge-ratio-by-zone-store";

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> rawRequests = builder.stream(
                Topics.RIDE_REQUESTS,
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> rawLocations = builder.stream(
                Topics.DRIVER_LOCATIONS,
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> rawTrips = builder.stream(
                Topics.TRIP_EVENTS,
                Consumed.with(Serdes.String(), Serdes.String()));

        // -----------------------------------------------------------------
        // DISP-1 - Ingestion fiable (les TROIS flux)
        //   Parser (model.RideRequest / DriverLocation / TripEvent), valider
        //   (dont bornes GPS Paris : lat 48.7-49.0, lon 2.2-2.5), router les
        //   invalides vers Topics.DLQ avec message original + raison.
        //   Meme parse deux-phases + chaine de regles metier a court-circuit
        //   pour les trois flux (cf. common.EventValidator, ADR 0004),
        //   regles propres a chaque type dans validation.*Validation.
        // -----------------------------------------------------------------

        // Flux types, exploites par DISP-2/3/5+ (demande/offre par zone,
        // courses anormales...).
        KStream<String, RideRequest> validRideRequests = validateAndRouteToDlq(
                rawRequests, RideRequestValidation.VALIDATOR, Topics.RIDE_REQUESTS);

        KStream<String, DriverLocation> validDriverLocations = validateAndRouteToDlq(
                rawLocations, DriverLocationValidation.VALIDATOR, Topics.DRIVER_LOCATIONS);

        // Correlation START/END (orphan END) hors-perimetre ici,
        // cf. ADR 0003 / TripEventValidation.
        KStream<String, TripEvent> validTripEvents = validateAndRouteToDlq(
                rawTrips, TripEventValidation.VALIDATOR, Topics.TRIP_EVENTS);

        // DISP-2 - Demande par zone (tumbling 2 min)     -> Topics.DEMAND_BY_ZONE
        KTable<Windowed<String>, Long> demandByZone = buildDemandByZone(validRideRequests);

        // DISP-3 - Offre par zone (KTable des chauffeurs) -> Topics.SUPPLY_BY_ZONE
        KTable<String, Long> supplyByZone = publishSupplyByZone(validDriverLocations);

        // DISP-4 - Surge pricing (demande/offre > 2)     -> Topics.SURGE
        buildSurge(demandByZone, supplyByZone);
        // DISP-5 - Courses anormales (vitesse, duree)    -> Topics.ALERTS_TRIPS
        // DISP-6 (bonus) - API REST Interactive Queries : GET /api/v1/surge/{zone}

        return builder.build();
    }

    /**
     * DISP-2 (issue #6) : compte les demandes de course par pickup.zone
     * (pas par la cle du record, qui est user_id) sur des fenetres
     * tumbling de 2 min avec 6 min de grace (meme seuil que le retard
     * accepte en validation, cf. CONTEXT.md). Pas de suppress() : chaque
     * mise a jour du compte est publiee des qu'elle arrive (ADR 0001),
     * pour que DISP-4 reagisse a une rafale pendant qu'elle se produit.
     * Retourne la KTable windowee (cle Windowed&lt;String&gt;) pour DISP-4,
     * qui doit d'abord l'aplatir en cle zone simple avant de la joindre a
     * l'offre (cf. buildSurge / ADR 0006).
     */
    private KTable<Windowed<String>, Long> buildDemandByZone(KStream<String, RideRequest> validRideRequests) {
        KTable<Windowed<String>, Long> demandByZone = validRideRequests
                .groupBy((key, request) -> request.pickup().zone(),
                        Grouped.with(Serdes.String(), JsonSerdes.of(RideRequest.class)))
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(2), Duration.ofMinutes(6)))
                .count(Materialized.with(Serdes.String(), Serdes.Long()));

        demandByZone.toStream()
                .map((windowedZone, count) -> {
                    DemandByZone demand = new DemandByZone(
                            windowedZone.key(),
                            Instant.ofEpochMilli(windowedZone.window().start()).toString(),
                            Instant.ofEpochMilli(windowedZone.window().end()).toString(),
                            count);
                    return KeyValue.pair(windowedZone.key(), JsonSerdes.toJson(demand));
                })
                .to(Topics.DEMAND_BY_ZONE, Produced.with(Serdes.String(), Serdes.String()));

        return demandByZone;
    }

    /**
     * DISP-3 - Offre par zone (issue #7). Le flux brut de pings ne peut pas
     * etre compte tel quel (un chauffeur emet ~1 ping/8s) : on materialise
     * d'abord la derniere position connue par chauffeur (KTable clee par
     * driver_id, "latest wins" via toTable()), puis on ne garde que les
     * chauffeurs AVAILABLE et on re-agrege par zone. C'est le point subtil
     * du ticket : KTable.groupBy() retire automatiquement l'ancienne
     * contribution d'un chauffeur (ancienne zone, ou statut plus AVAILABLE)
     * avant d'ajouter la nouvelle, donc un chauffeur qui change de zone ou
     * quitte AVAILABLE est decompte de sa zone precedente au lieu de
     * s'additionner en double. Retourne la KTable (cle zone) pour DISP-4
     * (cf. buildSurge / ADR 0006).
     */
    private KTable<String, Long> publishSupplyByZone(KStream<String, DriverLocation> validDriverLocations) {
        KTable<String, DriverLocation> latestLocationByDriver = validDriverLocations
                .toTable(Materialized.with(Serdes.String(), JsonSerdes.of(DriverLocation.class)));

        KTable<String, Long> supplyByZone = latestLocationByDriver
                .filter((driverId, location) -> "AVAILABLE".equals(location.status()))
                .groupBy((driverId, location) -> KeyValue.pair(location.zone(), location),
                        Grouped.with(Serdes.String(), JsonSerdes.of(DriverLocation.class)))
                .count(Materialized.with(Serdes.String(), Serdes.Long()));

        supplyByZone.toStream()
                .mapValues((zone, count) -> JsonSerdes.toJson(new SupplyByZone(zone, count)))
                .to(Topics.SUPPLY_BY_ZONE, Produced.with(Serdes.String(), Serdes.String()));

        return supplyByZone;
    }

    /**
     * DISP-4 (issue #8) : ratio demande/offre par zone. Aplatit d'abord la
     * demande windowee (cle Windowed&lt;String&gt;) en KTable cle par zone
     * simple (dernier compte connu, meme cle que l'offre), puis jointure
     * table-table demande/offre (ADR 0006) : le ratio se recalcule sur
     * toute mise a jour d'un des deux cotes, pas seulement la demande. Le
     * ratio non filtre est materialise pour toutes les zones dans
     * SURGE_RATIO_STORE (ADR 0002, consomme par DISP-6/issue #10) ; le
     * seuil (> 2) n'est applique qu'a la publication sur Topics.SURGE.
     */
    private void buildSurge(KTable<Windowed<String>, Long> demandByZone, KTable<String, Long> supplyByZone) {
        KTable<String, Long> currentDemandByZone = demandByZone.toStream()
                .selectKey((windowedZone, count) -> windowedZone.key())
                .repartition(Repartitioned.<String, Long>with(Serdes.String(), Serdes.Long())
                        .withName("surge-demand-by-zone"))
                .toTable(Materialized.with(Serdes.String(), Serdes.Long()));

        KTable<String, Double> surgeRatioByZone = currentDemandByZone.leftJoin(
                supplyByZone,
                (demand, supply) -> demand / (double) Math.max(supply == null ? 0L : supply, 1L),
                Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as(SURGE_RATIO_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Double()));

        surgeRatioByZone.toStream()
                .filter((zone, ratio) -> ratio > 2.0)
                .mapValues((zone, ratio) -> JsonSerdes.toJson(new SurgeEvent(zone, ratio)))
                .to(Topics.SURGE, Produced.with(Serdes.String(), Serdes.String()));
    }

    /**
     * Valide un flux brut via un EventValidator et le scinde en deux :
     * le flux type des messages valides (retourne, exploite par les
     * tickets suivants) et le flux des invalides, routes vers Topics.DLQ
     * sous forme de DlqEnvelope (cf. ADR 0004). Factorise le cablage
     * commun aux trois flux d'entree (issues #2/#3/#4).
     */
    private <T> KStream<String, T> validateAndRouteToDlq(
            KStream<String, String> raw, EventValidator<T> validator, String sourceTopic) {

        KStream<String, ValidationOutcome<T>> outcomes = raw.mapValues(validator::validate);

        KStream<String, T> valid = outcomes
                .filter((key, outcome) -> outcome instanceof ValidationOutcome.Valid<T>)
                .mapValues(outcome -> ((ValidationOutcome.Valid<T>) outcome).value());

        outcomes
                .filter((key, outcome) -> outcome instanceof ValidationOutcome.Invalid<T>)
                .map((key, outcome) -> {
                    ValidationOutcome.Invalid<T> invalid = (ValidationOutcome.Invalid<T>) outcome;
                    DlqEnvelope envelope = new DlqEnvelope(
                            sourceTopic, key, invalid.raw(), invalid.reason(),
                            Instant.now().toString());
                    return KeyValue.pair(key, JsonSerdes.toJson(envelope));
                })
                .to(Topics.DLQ, Produced.with(Serdes.String(), Serdes.String()));

        return valid;
    }
}

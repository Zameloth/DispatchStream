package fr.esgi.kafka.dispatch;

import fr.esgi.kafka.dispatch.common.DlqEnvelope;
import fr.esgi.kafka.dispatch.common.EventValidator;
import fr.esgi.kafka.dispatch.common.JsonSerdes;
import fr.esgi.kafka.dispatch.common.ValidationOutcome;
import fr.esgi.kafka.dispatch.model.DriverLocation;
import fr.esgi.kafka.dispatch.model.RideRequest;
import fr.esgi.kafka.dispatch.model.SupplyByZone;
import fr.esgi.kafka.dispatch.model.TripEvent;
import fr.esgi.kafka.dispatch.validation.DriverLocationValidation;
import fr.esgi.kafka.dispatch.validation.RideRequestValidation;
import fr.esgi.kafka.dispatch.validation.TripEventValidation;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Instant;

/**
 * Construisez ici la topologie du projet DISPATCH.
 * Quarkus detecte ce @Produces Topology et demarre Kafka Streams
 * automatiquement (extension quarkus-kafka-streams).
 * Chaque ticket du backlog (README) correspond a un bloc ci-dessous.
 */
@ApplicationScoped
public class TopologyProducer {

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
        // DISP-3 - Offre par zone (KTable des chauffeurs) -> Topics.SUPPLY_BY_ZONE
        publishSupplyByZone(validDriverLocations);
        // DISP-4 - Surge pricing (demande/offre > 2)     -> Topics.SURGE
        // DISP-5 - Courses anormales (vitesse, duree)    -> Topics.ALERTS_TRIPS
        // DISP-6 (bonus) - API REST Interactive Queries : GET /api/v1/surge/{zone}

        return builder.build();
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
     * s'additionner en double.
     */
    private void publishSupplyByZone(KStream<String, DriverLocation> validDriverLocations) {
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

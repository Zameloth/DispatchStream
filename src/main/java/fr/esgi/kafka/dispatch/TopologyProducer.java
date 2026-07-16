package fr.esgi.kafka.dispatch;

import fr.esgi.kafka.dispatch.common.DlqEnvelope;
import fr.esgi.kafka.dispatch.common.JsonSerdes;
import fr.esgi.kafka.dispatch.common.ValidationOutcome;
import fr.esgi.kafka.dispatch.model.DriverLocation;
import fr.esgi.kafka.dispatch.model.RideRequest;
import fr.esgi.kafka.dispatch.model.TripEvent;
import fr.esgi.kafka.dispatch.validation.DriverLocationValidation;
import fr.esgi.kafka.dispatch.validation.RideRequestValidation;
import fr.esgi.kafka.dispatch.validation.TripEventValidation;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
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

        // Sanity check de demarrage : verifiez la connexion au cluster,
        // puis SUPPRIMEZ ce peek (il pollue les logs et coute cher).
        rawLocations.peek((key, value) -> Log.infof("[dispatch] %s -> %s", key, value));

        // -----------------------------------------------------------------
        // DISP-1 - Ingestion fiable (les TROIS flux)
        //   Parser (model.RideRequest / DriverLocation / TripEvent), valider
        //   (dont bornes GPS Paris : lat 48.7-49.0, lon 2.2-2.5), router les
        //   invalides vers Topics.DLQ avec message original + raison.
        //   Pistes : split()/branch(), JsonSerdes.parseOrNull(...).
        // -----------------------------------------------------------------

        // dispatch.ride.requests : parse deux-phases + chaine de regles
        // metier a court-circuit (cf. common.EventValidator, ADR 0004),
        // regles propres a RideRequest dans validation.RideRequestValidation.
        KStream<String, ValidationOutcome<RideRequest>> rideRequestOutcomes =
                rawRequests.mapValues(RideRequestValidation.VALIDATOR::validate);

        // Flux type, exploite par DISP-2/3+ (demande/offre par zone...).
        KStream<String, RideRequest> validRideRequests = rideRequestOutcomes
                .filter((key, outcome) -> outcome instanceof ValidationOutcome.Valid<RideRequest>)
                .mapValues(outcome -> ((ValidationOutcome.Valid<RideRequest>) outcome).value());

        rideRequestOutcomes
                .filter((key, outcome) -> outcome instanceof ValidationOutcome.Invalid<RideRequest>)
                .map((key, outcome) -> {
                    ValidationOutcome.Invalid<RideRequest> invalid =
                            (ValidationOutcome.Invalid<RideRequest>) outcome;
                    DlqEnvelope envelope = new DlqEnvelope(
                            Topics.RIDE_REQUESTS, key, invalid.raw(), invalid.reason(),
                            Instant.now().toString());
                    return KeyValue.pair(key, JsonSerdes.toJson(envelope));
                })
                .to(Topics.DLQ, Produced.with(Serdes.String(), Serdes.String()));

        // dispatch.driver.locations : meme parse deux-phases + chaine de
        // regles metier a court-circuit (cf. common.EventValidator, ADR
        // 0004), regles propres a DriverLocation dans
        // validation.DriverLocationValidation.
        KStream<String, ValidationOutcome<DriverLocation>> driverLocationOutcomes =
                rawLocations.mapValues(DriverLocationValidation.VALIDATOR::validate);

        // Flux type, exploite par DISP-3+ (offre par zone...).
        KStream<String, DriverLocation> validDriverLocations = driverLocationOutcomes
                .filter((key, outcome) -> outcome instanceof ValidationOutcome.Valid<DriverLocation>)
                .mapValues(outcome -> ((ValidationOutcome.Valid<DriverLocation>) outcome).value());

        driverLocationOutcomes
                .filter((key, outcome) -> outcome instanceof ValidationOutcome.Invalid<DriverLocation>)
                .map((key, outcome) -> {
                    ValidationOutcome.Invalid<DriverLocation> invalid =
                            (ValidationOutcome.Invalid<DriverLocation>) outcome;
                    DlqEnvelope envelope = new DlqEnvelope(
                            Topics.DRIVER_LOCATIONS, key, invalid.raw(), invalid.reason(),
                            Instant.now().toString());
                    return KeyValue.pair(key, JsonSerdes.toJson(envelope));
                })
                .to(Topics.DLQ, Produced.with(Serdes.String(), Serdes.String()));

        // dispatch.trip.events : meme parse deux-phases + chaine de regles
        // metier a court-circuit, regles propres a TripEvent dans
        // validation.TripEventValidation. Correlation START/END (orphan
        // END) hors-perimetre ici, cf. ADR 0003 / TripEventValidation.
        KStream<String, ValidationOutcome<TripEvent>> tripEventOutcomes =
                rawTrips.mapValues(TripEventValidation.VALIDATOR::validate);

        // Flux type, exploite par DISP-5 (courses anormales...).
        KStream<String, TripEvent> validTripEvents = tripEventOutcomes
                .filter((key, outcome) -> outcome instanceof ValidationOutcome.Valid<TripEvent>)
                .mapValues(outcome -> ((ValidationOutcome.Valid<TripEvent>) outcome).value());

        tripEventOutcomes
                .filter((key, outcome) -> outcome instanceof ValidationOutcome.Invalid<TripEvent>)
                .map((key, outcome) -> {
                    ValidationOutcome.Invalid<TripEvent> invalid =
                            (ValidationOutcome.Invalid<TripEvent>) outcome;
                    DlqEnvelope envelope = new DlqEnvelope(
                            Topics.TRIP_EVENTS, key, invalid.raw(), invalid.reason(),
                            Instant.now().toString());
                    return KeyValue.pair(key, JsonSerdes.toJson(envelope));
                })
                .to(Topics.DLQ, Produced.with(Serdes.String(), Serdes.String()));

        // DISP-2 - Demande par zone (tumbling 2 min)     -> Topics.DEMAND_BY_ZONE
        // DISP-3 - Offre par zone (KTable des chauffeurs) -> Topics.SUPPLY_BY_ZONE
        // DISP-4 - Surge pricing (demande/offre > 2)     -> Topics.SURGE
        // DISP-5 - Courses anormales (vitesse, duree)    -> Topics.ALERTS_TRIPS
        // DISP-6 (bonus) - API REST Interactive Queries : GET /api/v1/surge/{zone}

        return builder.build();
    }
}

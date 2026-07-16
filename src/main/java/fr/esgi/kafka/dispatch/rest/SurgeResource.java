package fr.esgi.kafka.dispatch.rest;

import fr.esgi.kafka.dispatch.TopologyProducer;
import fr.esgi.kafka.dispatch.common.Zones;
import fr.esgi.kafka.dispatch.model.SurgeStatus;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Map;

/**
 * Bonus DISP-6 (issue #10) : expose le surge courant par zone via
 * Interactive Queries, en lisant TopologyProducer.SURGE_RATIO_STORE - le
 * ratio non filtre materialise pour les 12 zones (ADR 0002), pas le
 * topic dispatch.surge qui n'a que les incidents ayant franchi le seuil.
 * La logique de lecture (ratioResponse) est separee du champ streams
 * injecte pour rester testable avec un simple KeyValueStore issu d'un
 * TopologyTestDriver, sans broker Kafka ni framework de mock.
 */
@Path("/api/v1/surge")
@Produces(MediaType.APPLICATION_JSON)
public class SurgeResource {

    @Inject
    KafkaStreams streams;

    @GET
    @Path("/{zone}")
    public Response surge(@PathParam("zone") String zone) {
        if (!Zones.KNOWN_ZONES.contains(zone)) {
            return errorResponse(Response.Status.NOT_FOUND, "unknown zone: " + zone);
        }

        try {
            return ratioResponse(store(), zone);
        } catch (InvalidStateStoreException e) {
            // Store pas encore interrogeable (streams en demarrage/rebalance) :
            // 503 plutot qu'une 500/stack trace (critere d'acceptation du ticket).
            return errorResponse(Response.Status.SERVICE_UNAVAILABLE, "surge store not ready yet");
        }
    }

    private static Response errorResponse(Response.Status status, String message) {
        // Passe par Jackson (Map, pas une String concatenee a la main) pour
        // qu'un zone arbitraire dans l'URL ne puisse pas casser le JSON.
        return Response.status(status).entity(Map.of("error", message)).build();
    }

    private ReadOnlyKeyValueStore<String, Double> store() {
        return streams.store(StoreQueryParameters.fromNameAndType(
                TopologyProducer.SURGE_RATIO_STORE, QueryableStoreTypes.keyValueStore()));
    }

    static Response ratioResponse(ReadOnlyKeyValueStore<String, Double> store, String zone) {
        Double ratio = store.get(zone);
        double value = ratio == null ? 0.0 : ratio;
        return Response.ok(new SurgeStatus(zone, value, value > TopologyProducer.SURGE_THRESHOLD)).build();
    }
}

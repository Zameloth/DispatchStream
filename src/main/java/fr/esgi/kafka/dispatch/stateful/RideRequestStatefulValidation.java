package fr.esgi.kafka.dispatch.stateful;

import fr.esgi.kafka.dispatch.common.JsonSerdes;
import fr.esgi.kafka.dispatch.common.Timestamps;
import fr.esgi.kafka.dispatch.common.ValidationOutcome;
import fr.esgi.kafka.dispatch.model.RideRequest;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.time.Instant;

/**
 * Detection late/duplicate pour RideRequest, identite = request_id
 * (CONTEXT.md "Duplicate event"). Premiere etape stateful du pipeline
 * (ADR 0007), executee apres la validation stateless (issue #2) sur le
 * flux deja bien forme : late d'abord (timestamp du record vs timestamp
 * metier, sans etat), puis duplicate via le store keye par request_id.
 * Pas de categorie "conflicting" pour ce flux (reservee aux positions
 * chauffeur, cf. CONTEXT.md) : une meme identite dont le contenu differe
 * est laissee valide (choix de perimetre assume, cf. ADR 0007).
 */
public final class RideRequestStatefulValidation {

    public static final String STORE_NAME = "ride-request-seen-store";

    public static final StoreBuilder<KeyValueStore<String, RideRequestOccurrence>> STORE_BUILDER =
            Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore(STORE_NAME),
                    Serdes.String(),
                    JsonSerdes.of(RideRequestOccurrence.class));

    private RideRequestStatefulValidation() {
    }

    public static FixedKeyProcessorSupplier<String, ValidationOutcome.Valid<RideRequest>, ValidationOutcome<RideRequest>> processorSupplier() {
        return Processor::new;
    }

    private static final class Processor
            implements FixedKeyProcessor<String, ValidationOutcome.Valid<RideRequest>, ValidationOutcome<RideRequest>> {

        private FixedKeyProcessorContext<String, ValidationOutcome<RideRequest>> context;
        private KeyValueStore<String, RideRequestOccurrence> store;

        @Override
        public void init(FixedKeyProcessorContext<String, ValidationOutcome<RideRequest>> context) {
            this.context = context;
            this.store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(FixedKeyRecord<String, ValidationOutcome.Valid<RideRequest>> record) {
            ValidationOutcome.Valid<RideRequest> outcome = record.value();
            RideRequest request = outcome.value();
            Instant arrivedAt = Instant.ofEpochMilli(record.timestamp());

            if (Timestamps.isLate(request.timestamp(), arrivedAt)) {
                forward(record, ValidationOutcome.invalid(outcome.raw(), "late"));
                return;
            }

            RideRequestOccurrence previous = store.get(request.requestId());
            if (previous != null && !isExpired(previous, arrivedAt) && previous.request().equals(request)) {
                forward(record, ValidationOutcome.invalid(outcome.raw(), "duplicate"));
                return;
            }

            store.put(request.requestId(), new RideRequestOccurrence(request, arrivedAt.toEpochMilli()));
            forward(record, outcome);
        }

        private boolean isExpired(RideRequestOccurrence previous, Instant arrivedAt) {
            Duration age = Duration.between(Instant.ofEpochMilli(previous.seenAtEpochMilli()), arrivedAt);
            return age.compareTo(Timestamps.LATE_THRESHOLD) > 0;
        }

        private void forward(
                FixedKeyRecord<String, ValidationOutcome.Valid<RideRequest>> record,
                ValidationOutcome<RideRequest> value) {
            context.forward(record.withValue(value));
        }

        @Override
        public void close() {
        }
    }
}

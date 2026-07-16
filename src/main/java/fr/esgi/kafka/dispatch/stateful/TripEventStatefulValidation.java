package fr.esgi.kafka.dispatch.stateful;

import fr.esgi.kafka.dispatch.common.JsonSerdes;
import fr.esgi.kafka.dispatch.common.Timestamps;
import fr.esgi.kafka.dispatch.common.ValidationOutcome;
import fr.esgi.kafka.dispatch.model.TripEvent;
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
 * Detection late/duplicate/orphan-END pour TripEvent, identite =
 * (trip_id, event) (CONTEXT.md "Duplicate event"/"Orphan END event").
 * Completes ADR 0003's deferral (correlation START/END). Un seul store,
 * keye par "trip_id|event" : sert au dedup (lookup sur sa propre cle) et
 * a la correlation orphan-END (lookup sur "trip_id|START" quand
 * l'evenement traite est un END), evitant un second store (ADR 0007).
 * Pas de categorie "conflicting" pour ce flux (cf. RideRequestStatefulValidation) :
 * une meme identite dont le contenu differe est laissee valide.
 */
public final class TripEventStatefulValidation {

    public static final String STORE_NAME = "trip-event-seen-store";
    private static final String START = "START";

    public static final StoreBuilder<KeyValueStore<String, TripEventOccurrence>> STORE_BUILDER =
            Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore(STORE_NAME),
                    Serdes.String(),
                    JsonSerdes.of(TripEventOccurrence.class));

    private TripEventStatefulValidation() {
    }

    public static FixedKeyProcessorSupplier<String, ValidationOutcome.Valid<TripEvent>, ValidationOutcome<TripEvent>> processorSupplier() {
        return Processor::new;
    }

    private static String identityKey(String tripId, String event) {
        return tripId + "|" + event;
    }

    private static final class Processor
            implements FixedKeyProcessor<String, ValidationOutcome.Valid<TripEvent>, ValidationOutcome<TripEvent>> {

        private FixedKeyProcessorContext<String, ValidationOutcome<TripEvent>> context;
        private KeyValueStore<String, TripEventOccurrence> store;

        @Override
        public void init(FixedKeyProcessorContext<String, ValidationOutcome<TripEvent>> context) {
            this.context = context;
            this.store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(FixedKeyRecord<String, ValidationOutcome.Valid<TripEvent>> record) {
            ValidationOutcome.Valid<TripEvent> outcome = record.value();
            TripEvent event = outcome.value();
            Instant arrivedAt = Instant.ofEpochMilli(record.timestamp());

            if (Timestamps.isLate(event.timestamp(), arrivedAt)) {
                forward(record, ValidationOutcome.invalid(outcome.raw(), "late"));
                return;
            }

            String key = identityKey(event.tripId(), event.event());
            TripEventOccurrence previous = store.get(key);
            if (previous != null && !isExpired(previous, arrivedAt) && previous.event().equals(event)) {
                forward(record, ValidationOutcome.invalid(outcome.raw(), "duplicate"));
                return;
            }

            if ("END".equals(event.event()) && !hasMatchingStart(event.tripId(), arrivedAt)) {
                forward(record, ValidationOutcome.invalid(outcome.raw(), "no matching START for trip_id"));
                return;
            }

            store.put(key, new TripEventOccurrence(event, arrivedAt.toEpochMilli()));
            forward(record, outcome);
        }

        private boolean hasMatchingStart(String tripId, Instant arrivedAt) {
            TripEventOccurrence start = store.get(identityKey(tripId, START));
            return start != null && !isExpired(start, arrivedAt);
        }

        private boolean isExpired(TripEventOccurrence previous, Instant arrivedAt) {
            Duration age = Duration.between(Instant.ofEpochMilli(previous.seenAtEpochMilli()), arrivedAt);
            return age.compareTo(Timestamps.LATE_THRESHOLD) > 0;
        }

        private void forward(
                FixedKeyRecord<String, ValidationOutcome.Valid<TripEvent>> record,
                ValidationOutcome<TripEvent> value) {
            context.forward(record.withValue(value));
        }

        @Override
        public void close() {
        }
    }
}

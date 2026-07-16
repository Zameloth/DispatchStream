package fr.esgi.kafka.dispatch.stateful;

import fr.esgi.kafka.dispatch.common.JsonSerdes;
import fr.esgi.kafka.dispatch.common.Timestamps;
import fr.esgi.kafka.dispatch.common.ValidationOutcome;
import fr.esgi.kafka.dispatch.model.DriverLocation;
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
import java.util.Objects;

/**
 * Detection late/duplicate/conflicting pour DriverLocation, identite =
 * (driver_id, timestamp) (CONTEXT.md "Duplicate event"/"Conflicting
 * event"). Meme structure que RideRequestStatefulValidation (ADR 0007) :
 * late d'abord, puis lookup dans le store keye par l'identite composite.
 * Le "contenu" compare est lat/lon uniquement (celui que CONTEXT.md
 * qualifie explicitement de contradictoire en cas de desaccord) : meme
 * lat/lon -> duplicate, lat/lon different -> conflicting.
 */
public final class DriverLocationStatefulValidation {

    public static final String STORE_NAME = "driver-location-seen-store";

    public static final StoreBuilder<KeyValueStore<String, DriverLocationOccurrence>> STORE_BUILDER =
            Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore(STORE_NAME),
                    Serdes.String(),
                    JsonSerdes.of(DriverLocationOccurrence.class));

    private DriverLocationStatefulValidation() {
    }

    public static FixedKeyProcessorSupplier<String, ValidationOutcome.Valid<DriverLocation>, ValidationOutcome<DriverLocation>> processorSupplier() {
        return Processor::new;
    }

    private static String identityKey(DriverLocation location) {
        return location.driverId() + "|" + location.timestamp();
    }

    private static boolean sameLocation(DriverLocation a, DriverLocation b) {
        return Objects.equals(a.lat(), b.lat()) && Objects.equals(a.lon(), b.lon());
    }

    private static final class Processor
            implements FixedKeyProcessor<String, ValidationOutcome.Valid<DriverLocation>, ValidationOutcome<DriverLocation>> {

        private FixedKeyProcessorContext<String, ValidationOutcome<DriverLocation>> context;
        private KeyValueStore<String, DriverLocationOccurrence> store;

        @Override
        public void init(FixedKeyProcessorContext<String, ValidationOutcome<DriverLocation>> context) {
            this.context = context;
            this.store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(FixedKeyRecord<String, ValidationOutcome.Valid<DriverLocation>> record) {
            ValidationOutcome.Valid<DriverLocation> outcome = record.value();
            DriverLocation location = outcome.value();
            Instant arrivedAt = Instant.ofEpochMilli(record.timestamp());

            if (Timestamps.isLate(location.timestamp(), arrivedAt)) {
                forward(record, ValidationOutcome.invalid(outcome.raw(), "late"));
                return;
            }

            String key = identityKey(location);
            DriverLocationOccurrence previous = store.get(key);
            if (previous != null && !isExpired(previous, arrivedAt)) {
                if (sameLocation(previous.location(), location)) {
                    forward(record, ValidationOutcome.invalid(outcome.raw(), "duplicate"));
                } else {
                    forward(record, ValidationOutcome.invalid(
                            outcome.raw(), "conflicting location for driver at timestamp"));
                }
                return;
            }

            store.put(key, new DriverLocationOccurrence(location, arrivedAt.toEpochMilli()));
            forward(record, outcome);
        }

        private boolean isExpired(DriverLocationOccurrence previous, Instant arrivedAt) {
            Duration age = Duration.between(Instant.ofEpochMilli(previous.seenAtEpochMilli()), arrivedAt);
            return age.compareTo(Timestamps.LATE_THRESHOLD) > 0;
        }

        private void forward(
                FixedKeyRecord<String, ValidationOutcome.Valid<DriverLocation>> record,
                ValidationOutcome<DriverLocation> value) {
            context.forward(record.withValue(value));
        }

        @Override
        public void close() {
        }
    }
}

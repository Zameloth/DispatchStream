package fr.esgi.kafka.dispatch.detection;

import fr.esgi.kafka.dispatch.model.GhostTripAlert;
import fr.esgi.kafka.dispatch.model.TripEvent;

import java.util.Optional;

/**
 * DISP-5 (issue #9) : evalue un END de course pour detecter un "ghost trip"
 * (cf. CONTEXT.md - vitesse moyenne = distance_km / (duration_seconds/3600)
 * depassant 130 km/h, ou duration_seconds depassant 10800). Signal de
 * suspicion de fraude, distinct des invalides DLQ de DISP-1 (TripEventValidation)
 * qui couvrent deja les valeurs manquantes/negatives/non-positives : seuls
 * des END bien formes (distance_km et duration_seconds garantis presents et
 * valides) atteignent cette methode, donc pas de division par zero possible.
 * Renvoie Optional, meme convention que ValidationStep/ValidationChain pour
 * un controle qui peut ne rien produire.
 */
public final class GhostTripDetection {

    private static final double MAX_AVERAGE_SPEED_KMH = 130.0;
    private static final int MAX_DURATION_SECONDS = 10_800;

    private GhostTripDetection() {
    }

    public static Optional<GhostTripAlert> evaluate(TripEvent event) {
        double averageSpeedKmh = event.distanceKm() / (event.durationSeconds() / 3600.0);

        String reason;
        if (averageSpeedKmh > MAX_AVERAGE_SPEED_KMH) {
            // Priorite vitesse si les deux seuils sont depasses : un seul
            // reason code par alerte (cf. GhostTripAlert), la vitesse est
            // le signal de fraude le plus fort des deux.
            reason = "excessive_speed";
        } else if (event.durationSeconds() > MAX_DURATION_SECONDS) {
            reason = "excessive_duration";
        } else {
            return Optional.empty();
        }

        return Optional.of(new GhostTripAlert(
                event.tripId(), event.driverId(), reason,
                averageSpeedKmh, event.distanceKm(), event.durationSeconds(), event.timestamp()));
    }
}

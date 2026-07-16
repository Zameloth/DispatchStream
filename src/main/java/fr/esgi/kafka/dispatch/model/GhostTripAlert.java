package fr.esgi.kafka.dispatch.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Valeur publiee sur Topics.ALERTS_TRIPS (issue #9) : un END de course
 * signale comme "ghost trip" (cf. CONTEXT.md - vitesse moyenne > 130 km/h
 * ou duree > 10800 s), avec la raison qui l'a declenche.
 */
public record GhostTripAlert(
        @JsonProperty("trip_id") String tripId,
        @JsonProperty("driver_id") String driverId,
        @JsonProperty("reason") String reason,
        @JsonProperty("average_speed_kmh") double averageSpeedKmh,
        @JsonProperty("distance_km") double distanceKm,
        @JsonProperty("duration_seconds") int durationSeconds,
        @JsonProperty("timestamp") String timestamp) {
}

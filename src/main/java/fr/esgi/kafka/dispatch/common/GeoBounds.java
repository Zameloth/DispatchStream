package fr.esgi.kafka.dispatch.common;

/**
 * Bornes de validite GPS pour ce projet (README.md / CONTEXT.md) : Paris
 * uniquement. Partagees par toute validation d'evenement qui reference des
 * coordonnees (RideRequest via pickup/dropoff, DriverLocation via lat/lon...),
 * pour eviter de dupliquer ces bornes dans chaque validation.*Validation.
 */
public final class GeoBounds {

    private static final double MIN_LAT = 48.7;
    private static final double MAX_LAT = 49.0;
    private static final double MIN_LON = 2.2;
    private static final double MAX_LON = 2.5;

    private GeoBounds() {
    }

    public static boolean containsParis(Double lat, Double lon) {
        if (lat == null || lon == null) {
            return false;
        }
        return lat >= MIN_LAT && lat <= MAX_LAT && lon >= MIN_LON && lon <= MAX_LON;
    }
}

package fr.esgi.kafka.dispatch.common;

import java.util.Set;

/**
 * Les 12 zones fixes de Paris (README.md, section "Les 12 zones") : liste
 * canonique donnee par le sujet, pas un choix arbitraire. Partagee par
 * toute validation d'evenement qui reference une zone (RideRequest via
 * pickup/dropoff, DriverLocation via zone...), pour eviter de dupliquer
 * cette liste dans chaque validation.*Validation (issues #2, #3, #4).
 */
public final class Zones {

    public static final Set<String> KNOWN_ZONES = Set.of(
            "Z-CHATELET", "Z-MONTMARTRE", "Z-BASTILLE", "Z-BERCY", "Z-DEFENSE",
            "Z-ETOILE", "Z-MONTPARNASSE", "Z-BELLEVILLE", "Z-ODEON", "Z-PIGALLE",
            "Z-NATION", "Z-AUTEUIL");

    private Zones() {
    }
}

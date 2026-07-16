package fr.esgi.kafka.dispatch.stateful;

import fr.esgi.kafka.dispatch.model.RideRequest;

/**
 * Derniere occurrence connue d'un request_id, memorisee par
 * RideRequestStatefulValidation pour la detection de duplicats
 * (cf. ADR 0007). seenAtEpochMilli est le timestamp d'arrivee (record
 * Kafka), pas le timestamp metier de la requete, pour dater l'expiration
 * de la fenetre de 6 min independamment du contenu.
 */
public record RideRequestOccurrence(RideRequest request, long seenAtEpochMilli) {
}

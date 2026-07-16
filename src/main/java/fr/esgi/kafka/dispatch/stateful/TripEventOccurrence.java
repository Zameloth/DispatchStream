package fr.esgi.kafka.dispatch.stateful;

import fr.esgi.kafka.dispatch.model.TripEvent;

/**
 * Derniere occurrence connue d'une identite (trip_id, event), memorisee
 * par TripEventStatefulValidation. Sert a la fois a la detection de
 * duplicats (meme cle) et a la correlation START/END (l'orphan-END lit
 * l'entree (trip_id, "START") de ce meme store, cf. ADR 0007).
 * seenAtEpochMilli est le timestamp d'arrivee (record Kafka), pas le
 * timestamp metier de l'evenement.
 */
public record TripEventOccurrence(TripEvent event, long seenAtEpochMilli) {
}

package fr.esgi.kafka.dispatch.common;

import java.util.Optional;
import java.util.function.Predicate;

/**
 * Un controle unitaire de validation metier sur une valeur deja typee T
 * (apres le parsing deux-phases de EventValidator). Renvoie le reason
 * code si la valeur echoue ce controle, ou Optional.empty() si elle le
 * passe. Chaque type d'evenement (RideRequest, DriverLocation,
 * TripEvent...) definit sa propre liste ordonnee de steps ; voir
 * ValidationChain pour l'enchainement a court-circuit.
 */
@FunctionalInterface
public interface ValidationStep<T> {

    Optional<String> check(T value);

    /** Construit un step a partir d'un predicat "est valide" et d'un reason code fixe. */
    static <T> ValidationStep<T> of(Predicate<T> isValid, String reasonIfInvalid) {
        return value -> isValid.test(value) ? Optional.empty() : Optional.of(reasonIfInvalid);
    }
}

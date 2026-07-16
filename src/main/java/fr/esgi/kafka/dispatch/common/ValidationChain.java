package fr.esgi.kafka.dispatch.common;

import java.util.List;
import java.util.Optional;

/**
 * Enchaine une liste ordonnee de ValidationStep et s'arrete a la premiere
 * qui echoue (court-circuit) : un seul reason code par evenement invalide,
 * cf. ADR 0004 "Reason precedence".
 */
public final class ValidationChain<T> {

    private final List<ValidationStep<T>> steps;

    public ValidationChain(List<ValidationStep<T>> steps) {
        this.steps = List.copyOf(steps);
    }

    /** Renvoie le reason code du premier step en echec, ou vide si tous passent. */
    public Optional<String> firstFailure(T value) {
        for (ValidationStep<T> step : steps) {
            Optional<String> reason = step.check(value);
            if (reason.isPresent()) {
                return reason;
            }
        }
        return Optional.empty();
    }
}

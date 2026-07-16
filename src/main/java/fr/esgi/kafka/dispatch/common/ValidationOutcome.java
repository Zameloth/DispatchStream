package fr.esgi.kafka.dispatch.common;

/**
 * Resultat de EventValidator#validate : soit une valeur typee valide,
 * soit une raison de rejet. Le message brut d'origine est toujours
 * conserve (necessaire pour construire un DlqEnvelope byte-for-byte en
 * cas de rejet, cf. ADR 0004).
 */
public sealed interface ValidationOutcome<T> {

    String raw();

    record Valid<T>(String raw, T value) implements ValidationOutcome<T> {
    }

    record Invalid<T>(String raw, String reason) implements ValidationOutcome<T> {
    }

    static <T> ValidationOutcome<T> valid(String raw, T value) {
        return new Valid<>(raw, value);
    }

    static <T> ValidationOutcome<T> invalid(String raw, String reason) {
        return new Invalid<>(raw, reason);
    }
}

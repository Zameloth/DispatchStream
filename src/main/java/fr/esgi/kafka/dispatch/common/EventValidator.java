package fr.esgi.kafka.dispatch.common;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Optional;

/**
 * Valide un message JSON brut en un type cible T, en deux temps
 * (cf. ADR 0004 "Two-phase parse") :
 *  1) parse generique (JsonNode) pour detecter un JSON syntaxiquement
 *     invalide (raison "malformed_json"), independamment de T ;
 *  2) conversion vers T pour detecter un champ au mauvais type
 *     (raison "wrong_type:<champ>") ;
 * puis applique une ValidationChain&lt;T&gt; de regles metier (champs
 * requis, enums, bornes...) qui s'arrete a la premiere raison trouvee
 * (cf. ADR 0004 "Reason precedence").
 *
 * Reutilisable pour tout type d'evenement du pipeline (RideRequest,
 * DriverLocation, TripEvent...) : fournir sa Class&lt;T&gt; et sa propre
 * liste ordonnee de ValidationStep&lt;T&gt;. Ne leve jamais d'exception :
 * toute entree, meme totalement invalide, produit un ValidationOutcome.
 */
public final class EventValidator<T> {

    public static final String MALFORMED_JSON = "malformed_json";
    private static final String ROOT_FIELD = "<root>";

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final Class<T> type;
    private final ValidationChain<T> chain;

    public EventValidator(Class<T> type, List<ValidationStep<T>> steps) {
        this.type = type;
        this.chain = new ValidationChain<>(steps);
    }

    public ValidationOutcome<T> validate(String raw) {
        if (raw == null || raw.isBlank()) {
            return ValidationOutcome.invalid(raw, MALFORMED_JSON);
        }

        JsonNode node;
        try {
            node = MAPPER.readTree(raw);
        } catch (Exception e) {
            return ValidationOutcome.invalid(raw, MALFORMED_JSON);
        }
        if (node == null || node.isNull() || node.isMissingNode()) {
            return ValidationOutcome.invalid(raw, MALFORMED_JSON);
        }

        T value;
        try {
            value = MAPPER.treeToValue(node, type);
        } catch (Exception e) {
            return ValidationOutcome.invalid(raw, "wrong_type:" + fieldNameOf(e));
        }
        if (value == null) {
            return ValidationOutcome.invalid(raw, MALFORMED_JSON);
        }

        Optional<String> reason = chain.firstFailure(value);
        if (reason.isPresent()) {
            return ValidationOutcome.invalid(raw, reason.get());
        }
        return ValidationOutcome.valid(raw, value);
    }

    /**
     * Reconstruit le chemin pointe (ex: "pickup.lat") a partir d'une
     * exception Jackson. Chemin vide = mismatch au niveau racine (le
     * message entier n'a pas la forme attendue, ex: un scalaire JSON nu
     * comme {@code 42} au lieu d'un objet).
     */
    private static String fieldNameOf(Exception e) {
        if (e instanceof JsonMappingException jme && !jme.getPath().isEmpty()) {
            StringBuilder path = new StringBuilder();
            for (JsonMappingException.Reference ref : jme.getPath()) {
                if (path.length() > 0) {
                    path.append('.');
                }
                if (ref.getFieldName() != null) {
                    path.append(ref.getFieldName());
                } else {
                    path.append('[').append(ref.getIndex()).append(']');
                }
            }
            return path.toString();
        }
        return ROOT_FIELD;
    }
}

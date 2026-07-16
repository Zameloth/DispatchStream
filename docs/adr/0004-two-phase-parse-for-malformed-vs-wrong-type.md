# Two-phase parse to distinguish malformed JSON from wrong-type fields

The anomaly table treats "JSON tronqué / non-JSON" and "Mauvais type" (e.g. `"lat": "douze"`) as distinct anomalies, but `JsonSerdes.parseOrNull` catches every Jackson exception into a single `null`, collapsing both into one outcome. We chose to **distinguish them**: parse into a generic `JsonNode` first to detect syntax validity, then convert that tree into the typed record to detect type mismatches, so the DLQ reason can say `malformed_json` vs `wrong_type:<field>` instead of one generic `unparseable`.

The trade-off: this is a two-phase parse instead of one `readValue` call, and materially more code than reusing `parseOrNull` as provided. We accepted the extra complexity because DISP-1 is graded on an anomaly table that lists these as separate rows, and a merged reason risks reading as an unhandled requirement at the oral rather than a deliberate simplification.

## Reason precedence

A single message can fail more than one check (e.g. missing `zone` and `lat` out of bounds). We chose **short-circuit on the first failure**, in a fixed order: JSON syntax → type conversion → required/non-null fields → enum membership → GPS bounds → timestamp parseable. One reason code per DLQ record, not an accumulated list. This keeps the reason schema a single string instead of an array, and matches what a human debugging the DLQ wants first; it also reflects a real dependency — GPS bounds can't be checked before confirming `lat`/`lon` exist and parsed as numbers.

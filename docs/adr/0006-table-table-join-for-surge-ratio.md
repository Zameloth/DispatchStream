# Table-table join for the surge ratio

DISP-4 needs `demand / max(supply, 1)` per zone, joining DISP-2's demand-by-zone
against DISP-3's supply-by-zone. Both sides are already tables — supply is a
`KTable<String, Long>` (latest `AVAILABLE` count per zone, issue #7); demand's
tumbling-window `KTable<Windowed<String>, Long>` also carries "current state,"
just windowed (ADR 0001's continuous emission keeps it up to date within the
window, record by record). We chose a **KTable-KTable join**, after flattening
demand's windowed key down to a plain zone key: `Windowed<String>` and `String`
don't co-partition, so `selectKey` + `repartition` + `toTable()` collapses
demand to "latest count for this zone" before the join — the pitfall the
README flags as "deux tables ... se joignent naturellement ; attention aux
fenêtres côté demande (les types diffèrent)." Flattening doesn't blunt ADR
0001's responsiveness: the flattened table still updates on every windowed
emission (one record per demand-count change), so a Bercy burst still
recomputes the ratio as it happens, not just once the window closes.

We deliberately used `leftJoin`, keyed by demand, rather than a stream-table
join (demand stream driving, supply table looked up). A stream-table join
would only recompute the ratio when a demand event arrives, silently missing
a zone drained of supply with no fresh demand to trigger a recompute — a
driver exodus from a zone could leave a stale, too-low ratio sitting in the
store indefinitely. The table-table join recomputes on updates from *either*
side: a demand burst or a supply drop both produce a fresh ratio, and it's
still the demand table driving inclusion (a zone only appears once it has
had at least one demand record), matching the "materialize on every demand
update" contract from ADR 0002. `leftJoin` (not inner) keeps a zone with zero
recorded supply from being dropped instead of computed — `max(supply, 1)`
treats a `null` right side as 0 supply, per CONTEXT.md's ratio formula.

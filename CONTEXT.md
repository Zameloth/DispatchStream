# DISPATCH

Real-time VTC (ride-hailing) marketplace pipeline for Paris, built on Quarkus + Kafka Streams. Consumes ride requests, driver GPS pings, and trip events; validates them, computes demand/supply per zone, triggers surge pricing, and flags anomalous ("ghost") trips.

## Language

### Validation & data quality

**Invalid event**:
A message the pipeline will not count in any aggregate. Covers structural/semantic breakage (missing/null required field, wrong type, GPS outside Paris bounds `48.7 ≤ lat ≤ 49.0` / `2.2 ≤ lon ≤ 2.5`, unrecognized enum, unparsable timestamp, truncated/malformed JSON, non-positive `duration_seconds` or negative `distance_km` on a trip `END`), lateness beyond the 6-minute threshold, duplicates, conflicts, and orphan `END`s (see below). Every invalid event is routed to the DLQ with the original message and a specific reason — nothing is silently dropped.
_Avoid_: Bad event, malformed event, rejected event (each describes only one category of invalidity).

**Late event**:
An event arriving more than 6 minutes after its own timestamp. Late events are invalid: routed to the DLQ with reason "late." Events arriving ≤6 min late are valid and processed normally — the demand-by-zone tumbling window's grace period is set to 6 min so every event that clears validation is guaranteed a window to land in.
_Avoid_: Straggler, delayed event.

**Duplicate event**:
A well-formed event repeating an identity already seen, with matching content, within a 6-minute deduplication window (matched to the late threshold: nothing surviving validation can be older than that, so a wider dedup window buys no additional coverage). Identity is defined per stream: ride requests by `request_id`; driver locations by `(driver_id, timestamp)`; trip events by `(trip_id, event)` (a `trip_id` may have at most one `START` and one `END`). Duplicates are invalid: routed to the DLQ with reason "duplicate."
_Avoid_: Duplicate request (too narrow — applies to all three streams).

**Conflicting event**:
Two events that share identity (same `(driver_id, timestamp)`) but disagree on content (different `lat`/`lon`) — a data-integrity contradiction, not a repeat of the same fact. Invalid: routed to the DLQ with reason "conflicting location for driver at timestamp."

**Orphan END event**:
A trip `END` event whose `trip_id` has no corresponding `START` seen within the 6-minute retention window (same bound as the late threshold, since a legitimate `START` can never be older than that by the time it would matter). Invalid: routed to the DLQ with reason "no matching START for trip_id," distinct from structural/semantic breakage.

### Marketplace metrics

**Zone**:
One of 12 fixed geographic areas of Paris (`Z-CHATELET`, `Z-MONTMARTRE`, etc.) — the unit of aggregation for demand, supply, and surge. Assigned upstream by the event source, not computed from lat/lon by this pipeline, but validated as a closed set at ingestion: a zone string outside the known 12 is an invalid event (unrecognized enum), since it would otherwise create a bogus aggregation key downstream.

**Demand**:
Count of valid ride requests per zone (by `pickup.zone`), aggregated over 2-minute tumbling windows.

**Supply**:
Count of drivers with status `AVAILABLE` per zone, based on each driver's latest known location. A driver leaving `AVAILABLE` (status change or zone change) is removed from their prior zone's count.
_Avoid_: Offer (French-ism from the README's "offre" — use "supply" consistently in code/English discussion).

**Surge**:
The condition where a zone's demand exceeds available supply by more than 2x (`demand / max(supply, 1) > 2`). Emitted as an event carrying the ratio.

**Ghost trip**:
A completed trip (`END` event) whose computed average speed (`distance_km / (duration_seconds / 3600)`) exceeds 130 km/h, or whose `duration_seconds` exceeds 10800 (3h) — a suspected fraud pattern, not a data-quality defect.

## Testing locally

The shared cluster used for real grading isn't reachable from a dev box, but a same-shape local run is: `docker-compose.yml` at the repo root brings up the same 3-broker KRaft cluster (Kafbat UI on `:8080`, bootstrap on `localhost:29092`), and `data/raw/*.jsonl` is a vendored copy of the course's real generated traffic for the three input topics (~192k messages, ~7% anomalies, one line per message as `{key, value, ts_ms}`).

1. `docker compose up -d`, then create the three input topics (the Quarkus Kafka Streams extension refuses to start until `quarkus.kafka-streams.topics` all exist) — either through Kafbat UI or `kafka-topics.sh --create`.
2. Build and run as a packaged jar, **not** `mvn quarkus:dev`: dev mode's Dev Services tries to auto-launch its own Kafka container because it only recognizes the plain `kafka.bootstrap.servers` property, not this project's `quarkus.kafka-streams.bootstrap-servers` — it'll fail trying to reach Docker instead of using the cluster you just started. Use `mvn package -DskipTests` then `GROUPE=<groupe> KAFKA_BOOTSTRAP=localhost:29092 java -jar target/quarkus-app/quarkus-run.jar`.
3. Replay realistic traffic instead of hand-crafting messages: `scripts/rejouer.py` (vendored from the course's generator toolkit; needs `pip install -r scripts/requirements.txt`) merges `data/raw/*.jsonl` chronologically and produces it to Kafka, topic name taken from each file's basename. `python scripts/rejouer.py --dossier data/raw --bootstrap localhost:29092` pushes all ~192k messages in a few seconds ("as fast as possible" mode — pass `--speed 1` for real-time pacing instead).
4. Verify: `kafka-consumer-groups.sh --describe --group dispatch-<groupe>` should settle at zero lag on all three input topics (proves every message was consumed, not just accepted); `kafka-console-consumer.sh --topic <groupe>.dispatch.dlq --from-beginning` should show one JSON envelope per rejected message with a specific `reason` — never a generic or missing one.

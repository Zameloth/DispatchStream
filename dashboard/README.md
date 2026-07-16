# DispatchStream dashboard

A standalone local Streamlit app to watch the DispatchStream pipeline
while it runs. Reads directly from Kafka (and, for later pages, the
surge interactive-query REST endpoint) — it doesn't touch the Quarkus
app.

## Run locally

Prerequisites: the docker-compose cluster running (`docker compose up
-d` from the repo root) and the Quarkus pipeline running against it
(see the root README's "Démarrage rapide").

```bash
pip install -r dashboard/requirements.txt

GROUPE=grp07 KAFKA_BOOTSTRAP=localhost:29092 streamlit run dashboard/app.py
```

`GROUPE` must match the group code the pipeline is running with (output
topics are `GROUPE`-prefixed); `KAFKA_BOOTSTRAP` defaults to
`localhost:29092`, the docker-compose cluster's bootstrap address, so
it can usually be omitted.

Then, from another terminal, replay traffic and watch it show up live:

```bash
python scripts/rejouer.py --dossier data/raw --bootstrap localhost:29092
```

## Pages

- **DLQ Live View** — rejected messages consumed from `<GROUPE>.dispatch.dlq`
  (source topic, key, reason, timestamp, raw payload) plus a live
  rejection-reason breakdown.

Later tickets add one page per remaining pipeline feature (demand/supply,
surge, ghost trips) under `pages/`.

## Adding a page

Reuse `config.py` for topic names/env vars and `kafka_consumer.py` for
the consumer pattern rather than re-solving reruns/threading:

```python
from config import DLQ  # or another topic constant
from kafka_consumer import get_topic_buffer

def decode(msg):
    ...  # bytes -> dict

buffer = get_topic_buffer(DLQ, decode)
records, aggregate_state = buffer.snapshot()
```

`get_topic_buffer` starts one background consumer thread per topic per
session (Streamlit reruns the script on every interaction, so the
consumer can't live inline in the script body) and feeds a bounded
deque; `on_message(record, state)` can optionally fold every message
into an unbounded aggregate (see the DLQ page's reason counts) that
survives the buffer's bound.

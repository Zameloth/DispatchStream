"""DLQ live view (issue #16): rejected messages + reason breakdown.

Built on the scaffold's Kafka-consumer pattern: a background thread
consumes config.DLQ into a bounded buffer for the table, and folds
every message into a per-reason count in the buffer's aggregate state
(unbounded, so the breakdown doesn't decay as older rows scroll off
the table).
"""
import json
import time

import pandas as pd
import streamlit as st

from config import DLQ
from kafka_consumer import get_topic_buffer

st.set_page_config(page_title="DLQ Live View", page_icon="🪦", layout="wide")
st.title("DLQ Live View")
st.caption(f"Consuming `{DLQ}`")

COLUMNS = ["source_topic", "key", "reason", "rejected_at", "raw_value"]


def decode(msg):
    envelope = json.loads(msg.value().decode("utf-8"))
    return {col: envelope.get(col) for col in COLUMNS}


def on_message(record, state):
    reason = record.get("reason") or "unknown"
    state[reason] = state.get(reason, 0) + 1


buffer = get_topic_buffer(DLQ, decode, maxlen=500, on_message=on_message)
records, reason_counts = buffer.snapshot()

if buffer.error:
    st.warning(f"Kafka consumer error: {buffer.error}")

table_col, breakdown_col = st.columns([2, 1])

with table_col:
    st.subheader(f"Rejected messages ({len(records)} most recent)")
    if records:
        df = pd.DataFrame(reversed(records))
        st.dataframe(
            df[COLUMNS], use_container_width=True, height=520, hide_index=True,
        )
    else:
        st.info("No DLQ messages yet — run scripts/rejouer.py to generate traffic.")

with breakdown_col:
    st.subheader("Rejection reasons")
    if reason_counts:
        counts = pd.Series(reason_counts, name="count").sort_values(ascending=False)
        st.bar_chart(counts)
        st.dataframe(counts, use_container_width=True)
    else:
        st.info("No data yet.")

refresh_seconds = st.sidebar.slider("Refresh interval (s)", 1, 10, 2)
time.sleep(refresh_seconds)
st.rerun()

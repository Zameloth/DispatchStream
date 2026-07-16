"""Kafka-consumer-in-Streamlit pattern shared by every dashboard page.

Streamlit reruns the whole page script on every interaction (including
our own auto-refresh reruns), so a page can't just create a
confluent_kafka.Consumer and poll() it inline: it would be recreated,
and its subscription lost, on every rerun. Instead, `get_topic_buffer`
starts one background thread per topic the first time a session visits
a page, and that thread owns the Consumer for the lifetime of the
session. The thread feeds a bounded, thread-safe deque (`TopicBuffer`);
the Streamlit script only ever reads a snapshot of it, so the buffer
(and the consumer's position) survives every rerun.

Pages can also pass `on_message(record, state)` to fold each decoded
record into a small aggregate (e.g. counts per reason) that isn't
subject to the buffer's bound, via `TopicBuffer.state`.
"""
import threading
import uuid
from collections import deque

import streamlit as st
from confluent_kafka import Consumer

from config import KAFKA_BOOTSTRAP


class TopicBuffer:
    """A background consumer for one topic, plus the buffer it feeds."""

    def __init__(self, topic, decode, maxlen=500, offset_reset="earliest",
                 on_message=None):
        self.topic = topic
        self.error = None
        self.state = {}
        self._buffer = deque(maxlen=maxlen)
        self._lock = threading.Lock()
        self._on_message = on_message
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run, args=(decode, offset_reset), daemon=True)
        self._thread.start()

    def _run(self, decode, offset_reset):
        try:
            consumer = Consumer({
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                # Unique group per session: each dashboard session gets
                # its own full view of the topic, independent of any
                # other open tab or previous run.
                "group.id": f"dashboard-{self.topic}-{uuid.uuid4()}",
                "auto.offset.reset": offset_reset,
                "enable.auto.commit": False,
            })
            consumer.subscribe([self.topic])
        except Exception as exc:
            with self._lock:
                self.error = f"could not start consumer for {self.topic}: {exc}"
            return

        try:
            while not self._stop.is_set():
                msg = consumer.poll(0.5)
                if msg is None:
                    continue
                if msg.error():
                    with self._lock:
                        self.error = str(msg.error())
                    continue
                try:
                    record = decode(msg)
                except Exception as exc:
                    record = {"decode_error": str(exc)}
                with self._lock:
                    self._buffer.append(record)
                    if self._on_message:
                        self._on_message(record, self.state)
        finally:
            consumer.close()

    def snapshot(self):
        """Return (records, aggregate_state) as of right now."""
        with self._lock:
            return list(self._buffer), dict(self.state)

    def stop(self):
        # Not wired to anything yet: Streamlit's stable API has no
        # session-end hook to call this from, so a session's consumer
        # thread runs until the process exits (it's a daemon thread, so
        # it won't block shutdown). Exposed for a page that wants to
        # explicitly retire a buffer, e.g. when switching topics.
        self._stop.set()


def get_topic_buffer(topic, decode, maxlen=500, offset_reset="earliest",
                      on_message=None):
    """Return this session's TopicBuffer for `topic`, starting it if needed."""
    key = f"_kafka_topic_buffer_{topic}"
    if key not in st.session_state:
        st.session_state[key] = TopicBuffer(
            topic, decode, maxlen=maxlen, offset_reset=offset_reset,
            on_message=on_message)
    return st.session_state[key]

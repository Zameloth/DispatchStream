"""DispatchStream dashboard entrypoint.

Run with: streamlit run dashboard/app.py

Pages live under dashboard/pages/, one per pipeline feature, and reuse
the config (config.py) and Kafka-consumer pattern (kafka_consumer.py)
defined at this level.
"""
import streamlit as st

from config import GROUPE, KAFKA_BOOTSTRAP

st.set_page_config(page_title="DispatchStream Dashboard", page_icon="📡",
                    layout="wide")

st.title("DispatchStream Dashboard")
st.markdown(
    "Local dev dashboard for the DispatchStream pipeline. Pick a page from "
    "the sidebar to watch a slice of the pipeline live while "
    "`scripts/rejouer.py` replays traffic."
)

st.subheader("Current configuration")
st.code(f"KAFKA_BOOTSTRAP = {KAFKA_BOOTSTRAP}\nGROUPE           = {GROUPE}",
        language="text")
st.caption(
    "Override with the KAFKA_BOOTSTRAP and GROUPE environment variables "
    "before `streamlit run dashboard/app.py` — same convention as "
    "Topics.java and scripts/rejouer.py. GROUPE must match the group the "
    "Quarkus pipeline is running with, since output topics are "
    "GROUPE-prefixed."
)

st.subheader("Pages")
st.markdown("- **DLQ Live View** — rejected messages and a "
            "rejection-reason breakdown, consumed from `<GROUPE>.dispatch.dlq`.")

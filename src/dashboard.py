from __future__ import annotations

import json
import os
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import streamlit as st
from confluent_kafka import Consumer, KafkaError

TOPICS = ["uav.imu", "uav.ran.clean", "uav.ran.noise", "uav.camera", "tos.events"]

TOPIC_LABELS = {
    "uav.imu": "UAV IMU",
    "uav.ran.clean": "RAN limpo",
    "uav.ran.noise": "RAN com ruído",
    "uav.camera": "Camera metadata",
    "tos.events": "TOS events",
}

TOPIC_METRICS = {
    "uav.imu": ["altitude_m", "speed_mps", "pitch_deg", "roll_deg", "yaw_deg"],
    "uav.ran.clean": ["sinr_db", "rsrp_dbm", "latency_ms", "bler"],
    "uav.ran.noise": ["sinr_db", "rsrp_dbm", "latency_ms", "bler"],
}

KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "group.id": os.getenv("KAFKA_GROUP_ID", "streamlit-kafka-dashboard"),
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

HISTORY_KEY = "topic_history"
FEED_KEY = "combined_feed"


def init_state(history_limit: int) -> None:
    if st.session_state.get("history_limit") != history_limit:
        st.session_state[HISTORY_KEY] = {topic: deque(maxlen=history_limit) for topic in TOPICS}
        st.session_state[FEED_KEY] = deque(maxlen=max(history_limit * 5, 250))
        st.session_state["history_limit"] = history_limit

    if HISTORY_KEY not in st.session_state:
        st.session_state[HISTORY_KEY] = {topic: deque(maxlen=history_limit) for topic in TOPICS}

    if FEED_KEY not in st.session_state:
        st.session_state[FEED_KEY] = deque(maxlen=max(history_limit * 5, 250))


def safe_json(value: bytes | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        decoded = json.loads(value.decode("utf-8"))
    except Exception:
        return {"raw_value": value.decode("utf-8", errors="replace")}
    return decoded if isinstance(decoded, dict) else {"value": decoded}


def message_timestamp(msg) -> str:
    payload = safe_json(msg.value())
    topic = msg.topic()

    if topic in {"uav.imu", "uav.ran.clean", "uav.ran.noise", "uav.camera"}:
        timestamp_s = payload.get("timestamp_s")
        if timestamp_s is not None:
            return f"{float(timestamp_s):.3f}s"

    if topic == "tos.events":
        timestamp = payload.get("timestamp")
        if timestamp:
            return str(timestamp)

    kafka_timestamp = msg.timestamp()
    if kafka_timestamp and kafka_timestamp[1] is not None:
        return datetime.fromtimestamp(kafka_timestamp[1] / 1000, tz=timezone.utc).isoformat()

    return "-"


def consume_updates(selected_topics: list[str], poll_seconds: float, max_messages: int) -> dict[str, list[dict[str, Any]]]:
    consumer = Consumer(KAFKA_CONFIG)
    topic_messages: dict[str, list[dict[str, Any]]] = {topic: [] for topic in selected_topics}

    try:
        consumer.subscribe(selected_topics)
        deadline = time.monotonic() + poll_seconds

        while time.monotonic() < deadline:
            remaining = max(0.1, deadline - time.monotonic())
            msg = consumer.poll(min(0.5, remaining))

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    st.warning(f"Kafka error: {msg.error()}")
                continue

            payload = safe_json(msg.value())
            topic = msg.topic()
            enriched_payload = {
                "topic": topic,
                "kafka_partition": msg.partition(),
                "kafka_offset": msg.offset(),
                "kafka_timestamp": message_timestamp(msg),
                "payload": payload,
            }

            topic_messages.setdefault(topic, []).append(enriched_payload)

            if len(topic_messages[topic]) >= max_messages:
                continue

        if any(topic_messages.values()):
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()

    return topic_messages


def flatten_topic_records(topic: str, messages: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for message in messages:
        payload = message.get("payload", {})
        if not isinstance(payload, dict):
            payload = {"value": payload}
        row = {"topic": topic, **message, **payload}
        row.pop("payload", None)
        rows.append(row)

    if not rows:
        return pd.DataFrame()

    dataframe = pd.DataFrame(rows)
    preferred_order = ["topic", "kafka_timestamp", "kafka_partition", "kafka_offset"]
    remaining_columns = [column for column in dataframe.columns if column not in preferred_order]
    return dataframe[preferred_order + remaining_columns]


def topic_family(topic: str) -> str:
    if topic == "uav.imu":
        return "imu"
    if topic in {"uav.ran.clean", "uav.ran.noise"}:
        return "ran"
    if topic == "uav.camera":
        return "camera"
    return "tos"


def render_overview(feed_df: pd.DataFrame, selected_topics: list[str], history: dict[str, deque]) -> None:
    st.subheader("Resumo em tempo real")

    columns = st.columns(len(selected_topics))
    for index, topic in enumerate(selected_topics):
        latest = history.get(topic, deque())
        count = len(latest)
        last_timestamp = latest[-1].get("kafka_timestamp", "-") if latest else "-"
        with columns[index]:
            st.metric(TOPIC_LABELS.get(topic, topic), count, help=f"Ultimo evento: {last_timestamp}")

    st.markdown("### Feed combinado")
    if feed_df.empty:
        st.info("Ainda não chegaram mensagens dos tópicos selecionados.")
    else:
        display_columns = [column for column in ["topic", "kafka_timestamp", "kafka_partition", "kafka_offset"] if column in feed_df.columns]
        extra_columns = [column for column in feed_df.columns if column not in display_columns]
        st.dataframe(feed_df[display_columns + extra_columns].tail(100), use_container_width=True, height=420)


def render_topic_chart(topic: str, topic_df: pd.DataFrame) -> None:
    family = topic_family(topic)

    if topic_df.empty:
        st.info("Sem mensagens neste tópico ainda.")
        return

    if family == "imu":
        chart_columns = [column for column in TOPIC_METRICS["uav.imu"] if column in topic_df.columns]
        numeric_df = topic_df[["kafka_timestamp", *chart_columns]].copy()
        for column in chart_columns:
            numeric_df[column] = pd.to_numeric(numeric_df[column], errors="coerce")
        numeric_df = numeric_df.dropna(subset=chart_columns, how="all")
        if not numeric_df.empty:
            st.line_chart(numeric_df.set_index("kafka_timestamp")[chart_columns])

    elif family == "ran":
        chart_columns = [column for column in TOPIC_METRICS[topic] if column in topic_df.columns]
        numeric_df = topic_df[["kafka_timestamp", *chart_columns]].copy()
        for column in chart_columns:
            numeric_df[column] = pd.to_numeric(numeric_df[column], errors="coerce")
        numeric_df = numeric_df.dropna(subset=chart_columns, how="all")
        if not numeric_df.empty:
            st.line_chart(numeric_df.set_index("kafka_timestamp")[chart_columns])

    elif family == "tos":
        if "event_type" in topic_df.columns:
            event_counts = topic_df["event_type"].value_counts().rename_axis("event_type").reset_index(name="count")
            st.bar_chart(event_counts.set_index("event_type"))

    elif family == "camera":
        if "frame_id" in topic_df.columns:
            camera_view = topic_df[[column for column in ["frame_id", "timestamp_s", "filename", "imu_ref_idx", "kafka_timestamp"] if column in topic_df.columns]]
            st.dataframe(camera_view.tail(20), use_container_width=True)


def render_topic_tab(topic: str, history: dict[str, deque]) -> None:
    st.subheader(TOPIC_LABELS.get(topic, topic))

    topic_messages = list(history.get(topic, deque()))
    topic_df = flatten_topic_records(topic, topic_messages)

    if topic_df.empty:
        st.info("Ainda sem dados neste tópico.")
        return

    render_topic_chart(topic, topic_df)

    st.markdown("### Mensagens recentes")
    st.dataframe(topic_df.tail(50), use_container_width=True, height=360)

    st.markdown("### Ultima mensagem")
    st.json(topic_df.iloc[-1].to_dict())


def render_footer() -> None:
    st.caption("Dashboard ligado aos tópicos Kafka publicados pelo replay dos datasets processed.")


def main() -> None:
    st.set_page_config(page_title="Kafka Topic Dashboard", layout="wide")
    st.title("Kafka Topic Dashboard")
    st.write("Monitoriza em tempo real os eventos que passam pelos tópicos Kafka do projeto.")

    with st.sidebar:
        st.header("Controlo")
        selected_topics = st.multiselect("Tópicos", options=TOPICS, default=TOPICS)
        history_limit = st.slider("Historico por tópico", min_value=20, max_value=1000, value=200, step=20)
        poll_seconds = st.slider("Tempo de leitura por ciclo (segundos)", min_value=0.5, max_value=5.0, value=1.0, step=0.5)
        auto_refresh = st.checkbox("Streaming contínuo ativo", value=True)
        max_messages_per_poll = st.slider("Max mensagens por tópico em cada ciclo", min_value=10, max_value=300, value=100, step=10)

        if st.button("Atualizar agora"):
            st.rerun()

    if not selected_topics:
        st.warning("Seleciona pelo menos um tópico para visualizar mensagens.")
        return

    init_state(history_limit)

    # 1. Fetch data seamlessly into session state
    new_messages = consume_updates(selected_topics, poll_seconds, max_messages_per_poll)

    history: dict[str, deque] = st.session_state[HISTORY_KEY]
    combined_feed: deque = st.session_state[FEED_KEY]

    for topic, messages in new_messages.items():
        for message in messages:
            message_copy = dict(message)
            history[topic].append(message_copy)
            combined_feed.append(message_copy)

    history_snapshot = {topic: history.get(topic, deque()) for topic in selected_topics}
    feed_rows = [record | {"topic": record.get("topic", "-")} for record in combined_feed]
    feed_df = pd.DataFrame(feed_rows)

    # 2. Render UI
    render_overview(feed_df, selected_topics, history_snapshot)

    st.markdown("---")
    tabs = st.tabs([TOPIC_LABELS.get(topic, topic) for topic in selected_topics])
    for tab, topic in zip(tabs, selected_topics):
        with tab:
            render_topic_tab(topic, history_snapshot)

    render_footer()

    # 3. Handle Auto-Refresh natively without full page reload flashes
    if auto_refresh:
        time.sleep(0.1) # short pause to keep your server CPU safe
        st.rerun()


if __name__ == "__main__":
    main()
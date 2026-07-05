from __future__ import annotations

import json
import os
from typing import Any

from confluent_kafka import Consumer, KafkaError


KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "group.id": os.getenv("KAFKA_GROUP_ID", "cyber-physical-perception-consumer"),
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
}

TOPICS = ["uav.imu", "uav.ran.clean", "uav.ran.noise", "uav.camera", "tos.events"]


def safe_payload(raw_value: bytes) -> dict[str, Any]:
    return json.loads(raw_value.decode("utf-8"))


def start_perception_bridge() -> None:
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe(TOPICS)

    print("[Kafka consumer] Active. Waiting for messages from the processed datasets...")

    last_imu_state: dict[str, Any] = {}
    last_ran_state: dict[str, Any] = {}

    try:
        while True:
            msg = consumer.poll(0.5)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[Kafka consumer] error: {msg.error()}")
                continue

            topic = msg.topic()
            payload = safe_payload(msg.value())

            if topic == "uav.imu":
                last_imu_state = {
                    "timestamp": payload.get("timestamp_s"),
                    "altitude": payload.get("altitude_m"),
                    "speed": payload.get("speed_mps"),
                    "roll": payload.get("roll_deg"),
                    "pitch": payload.get("pitch_deg"),
                    "yaw": payload.get("yaw_deg"),
                }
                print(
                    f"[uav.imu] t={last_imu_state.get('timestamp')} altitude={last_imu_state.get('altitude')} "
                    f"speed={last_imu_state.get('speed')} pitch={last_imu_state.get('pitch')}"
                )
            elif topic.startswith("uav.ran"):
                last_ran_state = {
                    "topic": topic,
                    "timestamp": payload.get("timestamp_s"),
                    "ue_id": payload.get("ue_id"),
                    "sinr": payload.get("sinr_db"),
                    "rsrp": payload.get("rsrp_dbm"),
                    "bler": payload.get("bler"),
                    "latency_ms": payload.get("latency_ms"),
                }
                print(
                    f"[{topic}] ue={last_ran_state.get('ue_id')} t={last_ran_state.get('timestamp')} "
                    f"sinr={last_ran_state.get('sinr')} rsrp={last_ran_state.get('rsrp')} bler={last_ran_state.get('bler')}"
                )
                if last_imu_state:
                    print(
                        f"  fusion -> altitude={last_imu_state.get('altitude')} pitch={last_imu_state.get('pitch')} "
                        f"with {last_ran_state.get('topic')}"
                    )
            elif topic == "uav.camera":
                print(
                    f"[uav.camera] frame_id={payload.get('frame_id')} filename={payload.get('filename')} "
                    f"imu_ref_idx={payload.get('imu_ref_idx')}"
                )
            elif topic == "tos.events":
                print(
                    f"[tos.events] event={payload.get('event_type')} entity={payload.get('entity_id')} "
                    f"terminal={payload.get('terminal')} timestamp={payload.get('timestamp')}"
                )
            else:
                print(f"[{topic}] {payload}")

    except KeyboardInterrupt:
        print("\n[Kafka consumer] shutting down...")
    finally:
        consumer.close()


if __name__ == "__main__":
    start_perception_bridge()
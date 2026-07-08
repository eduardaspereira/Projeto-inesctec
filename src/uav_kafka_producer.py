from __future__ import annotations

import argparse
import json
import os
import time
import random
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from confluent_kafka import Producer


BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "datasets" / "processed"
UAV_DIR = PROCESSED_DIR / "uav_data"
TOS_DIR = PROCESSED_DIR / "tos_data"

KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "client.id": "inesctec-processed-data-producer",
    "acks": "all",
}

TOPICS = {
    "imu": "uav.imu",
    "ran_clean": "uav.ran.clean",
    "ran_noise": "uav.ran.noise",
    "camera": "uav.camera",
    "tos": "tos.events",
}

DATASETS = [
    {
        "name": "imu",
        "path": UAV_DIR / "imu_data.csv",
        "topic": TOPICS["imu"],
        "sort_by": "timestamp_s",
        "key_fields": ("timestamp_s",),
    },
    {
        "name": "ran_clean",
        "path": UAV_DIR / "ran_kpms.csv",
        "topic": TOPICS["ran_clean"],
        "sort_by": "timestamp_s",
        "key_fields": ("ue_id", "timestamp_s", "report_idx"),
    },
    {
        "name": "ran_noise",
        "path": UAV_DIR / "ran_kpms_noise_ext.csv",
        "topic": TOPICS["ran_noise"],
        "sort_by": "timestamp_s",
        "key_fields": ("ue_id", "timestamp_s", "report_idx"),
    },
    {
        "name": "camera",
        "path": UAV_DIR / "images_metadata.json",
        "topic": TOPICS["camera"],
        "sort_by": "timestamp_s",
        "key_fields": ("frame_id", "timestamp_s", "filename"),
    },
    {
        "name": "tos",
        "path": TOS_DIR / "synthetic_port_events.csv",
        "topic": TOPICS["tos"],
        "sort_by": "timestamp",
        "key_fields": ("entity_id", "timestamp", "event_type"),
    },
]


def parse_stream_time(dataframe: pd.DataFrame, sort_by: str | None) -> pd.Series:
    if sort_by and sort_by in dataframe.columns:
        raw_values = dataframe[sort_by]

        numeric_values = pd.to_numeric(raw_values, errors="coerce")
        if numeric_values.notna().any():
            base_value = numeric_values.dropna().min()
            return (numeric_values - base_value).fillna(0.0)

        datetime_values = pd.to_datetime(raw_values, errors="coerce", utc=True)
        if datetime_values.notna().any():
            base_value = datetime_values.dropna().min()
            return (datetime_values - base_value).dt.total_seconds().fillna(0.0)

    return pd.Series(range(len(dataframe)), index=dataframe.index, dtype="float64")


def json_ready(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, dict):
        return {key: json_ready(inner_value) for key, inner_value in value.items()}
    if isinstance(value, list):
        return [json_ready(item) for item in value]
    if hasattr(value, "item"):
        return value.item()
    if pd.isna(value):
        return None
    return value


def load_dataset(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")

    if path.suffix.lower() == ".csv":
        dataframe = pd.read_csv(path)
    elif path.suffix.lower() == ".json":
        with path.open("r", encoding="utf-8") as file_handle:
            raw = json.load(file_handle)
        if isinstance(raw, dict) and "frames" in raw:
            dataframe = pd.DataFrame(raw["frames"])
        elif isinstance(raw, dict) and "samples" in raw:
            dataframe = pd.DataFrame(raw["samples"])
        else:
            dataframe = pd.DataFrame(raw)
    else:
        raise ValueError(f"Unsupported dataset format: {path.suffix}")

    return dataframe.where(pd.notnull(dataframe), None)


def build_message_key(record: dict[str, Any], key_fields: Iterable[str]) -> str:
    for field_name in key_fields:
        value = record.get(field_name)
        if value not in (None, ""):
            return str(value)
    return "record"


def delivery_report(error, message) -> None:
    if error is not None:
        print(f"[Kafka] delivery failed for {message.topic()}: {error}")


def apply_dqdm_and_fault_injection(record: dict[str, Any], topic: str) -> dict[str, Any]:
    # --- SOLUÇÃO 1: DQDM (Ruído Gaussiano Estocástico) ---
    if topic == "uav.ran.noise" and record.get("sinr_db") is not None:
        record["sinr_db"] += random.gauss(0, 0.5) 

    # --- SOLUÇÃO 2: Compound Stress-Testing (Injeção Determinística) ---
    timestamp = record.get("timestamp_s", 0)
    # Alterado para injetar o caos a meio do voo (entre os 4s e os 8s)
    if 4.0 <= timestamp <= 8.0:
        if topic == "uav.imu":
            record["pitch_deg"] = 45.0  # Força uma inclinação extrema
        if topic == "uav.ran.noise":
            # Usamos record.get() para evitar o erro NoneType de há bocado
            if record.get("sinr_db") is not None:
                record["sinr_db"] -= 15.0   # Força uma quebra drástica de sinal
            if record.get("bler") is not None:
                record["bler"] = 0.4        # Dispara a taxa de erro de bloco

    return record

def build_stream_events() -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []

    for dataset_order, dataset_spec in enumerate(DATASETS):
        dataframe = load_dataset(dataset_spec["path"])
        sort_by = dataset_spec.get("sort_by")
        if sort_by and sort_by in dataframe.columns:
            dataframe = dataframe.sort_values(by=sort_by, kind="stable")

        dataframe = dataframe.copy()
        dataframe["_stream_time"] = parse_stream_time(dataframe, sort_by)

        rows = dataframe.to_dict(orient="records")
        topic = dataset_spec["topic"]
        name = dataset_spec["name"]
        key_fields = dataset_spec.get("key_fields", ())

        for row_index, record in enumerate(rows):
            stream_time = float(record.pop("_stream_time", 0.0))
            
            # 🔥 Injeção aplicada aqui
            record = apply_dqdm_and_fault_injection(record, topic)
            
            payload = json.dumps(json_ready(record), ensure_ascii=False, separators=(",", ":"))
            events.append(
                {
                    "stream_time": stream_time,
                    "dataset_order": dataset_order,
                    "row_index": row_index,
                    "topic": topic,
                    "name": name,
                    "key": build_message_key(record, key_fields),
                    "payload": payload,
                }
            )

    events.sort(key=lambda event: (event["stream_time"], event["dataset_order"], event["row_index"]))
    return events

def publish_stream(producer: Producer, speedup: float, time_offset: float) -> float:
    events = build_stream_events()

    if not events:
        print("[Kafka] No processed data found to publish")
        return 0.0

    print(f"[Kafka] Streaming {len(events)} records. Cycle Time Offset: +{time_offset}s")

    sent = 0
    last_stream_time: float | None = None
    max_stream_time = 0.0

    try:
        for event in events:
            current_stream_time = float(event["stream_time"])
            max_stream_time = max(max_stream_time, current_stream_time)
            
            if last_stream_time is not None and speedup > 0:
                elapsed = max(current_stream_time - last_stream_time, 0.0)
                if elapsed > 0:
                    time.sleep(elapsed / speedup)

                # --- CORREÇÃO DO TIMESTAMP ---
                # Deserializa o payload original, atualiza o timestamp e volta a serializar
                payload_dict = json.loads(event["payload"])
                
                # SAFE CHECK: Garante que a chave existe e não é None antes de somar
                if "timestamp_s" in payload_dict and payload_dict["timestamp_s"] is not None:
                    payload_dict["timestamp_s"] = float(payload_dict["timestamp_s"]) + time_offset
                    
                if "timestamp" in payload_dict and payload_dict["timestamp"] is not None:
                    if isinstance(payload_dict["timestamp"], (int, float)):
                        payload_dict["timestamp"] = float(payload_dict["timestamp"]) + time_offset

            updated_payload = json.dumps(payload_dict, ensure_ascii=False, separators=(",", ":"))
            # ------------------------------

            producer.produce(
                topic=event["topic"],
                key=event["key"].encode("utf-8"),
                value=updated_payload.encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0.01)

            sent += 1
            last_stream_time = current_stream_time

            if sent % 100 == 0 or sent == len(events):
                print(f"[Kafka] Published {sent}/{len(events)} -> {event['topic']} ({event['name']})")
    finally:
        producer.flush()

    # Retorna a duração deste dataset para sabermos quanto somar no próximo ciclo
    return max_stream_time
def run_producer(speedup: float) -> None:
    producer = Producer(KAFKA_CONFIG)
    
    print("🔄 [Kafka] A iniciar injeção CÍCLICA de dados (Workaround para Demo)...")
    try:
        ciclo = 1
        accumulated_offset = 0.0
        
        while True:
            print(f"\n▶️ --- A iniciar Ciclo {ciclo} (Offset Acumulado: {accumulated_offset}s) ---")
            
            # publish_stream agora atualiza os dados e devolve o tempo máximo do voo
            cycle_duration = publish_stream(producer, speedup, accumulated_offset)
            
            print(f"⏸️ --- Fim do Ciclo {ciclo}. A reiniciar em 3 segundos... ---")
            producer.flush()
            time.sleep(3)
            
            # O offset do próximo ciclo será o fim do ciclo atual + os 3 segundos de pausa
            accumulated_offset += cycle_duration + 3.0
            ciclo += 1
            
    except KeyboardInterrupt:
        print("\n🛑 [Kafka] Simulador parado pelo utilizador.")
    finally:
        producer.flush()

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish the processed UAV and TOS datasets to Kafka topics.")
    parser.add_argument(
        "--speedup",
        type=float,
        default=20.0,
        help="Replay speed multiplier. Higher values make the simulated arrival faster.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_producer(args.speedup)
from __future__ import annotations
import json
import os
import time
from typing import Any
import numpy as np # Adicionado para gestão dos embeddings
from confluent_kafka import Consumer, KafkaError

KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "group.id": os.getenv("KAFKA_GROUP_ID", "cyber-physical-perception-consumer"),
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
}

TOPICS = ["uav.imu", "uav.ran.clean", "uav.ran.noise", "uav.camera", "tos.events"]

class MultimodalGlobalState:
    """Mantém a representação contínua do ambiente (Picture Pt) atualizada assincronamente."""
    def __init__(self):
        # Armazenamento das últimas percepções latentes recebidas
        self.latest_imu = np.zeros(128)
        self.latest_comms = np.zeros(128)
        self.latest_vision = np.zeros(512)
        self.last_timestamp = 0.0

    def _generate_fused_picture(self) -> np.ndarray:
        """Cria o embedding global de 768 dimensões por concatenação."""
        return np.concatenate((self.latest_imu, self.latest_comms, self.latest_vision))

    def update_modality(self, topic: str, payload: dict[str, Any]):
        """Atualiza a modalidade específica (Trigger Mode) em vez de Matching Mode."""
        # Nota: Na versão final, deves passar os payloads pelos encoders respetivos.
        # Aqui simulamos a atualização do estado mediante os dados recebidos.
        
        if topic == "uav.imu":
            self.last_timestamp = payload.get("timestamp_s", self.last_timestamp)
            print(f"[Estado Global] Atualizado via IMU (100Hz) a t={self.last_timestamp}")
            # self.latest_imu = imu_encoder(payload) 

        elif topic.startswith("uav.ran"):
            self.last_timestamp = payload.get("timestamp_s", self.last_timestamp)
            print(f"[Estado Global] Atualizado via RAN (t={self.last_timestamp})")
            # self.latest_comms = comms_encoder(payload)

        elif topic == "uav.camera":
            # Visão é tipicamente 1Hz
            print(f"[Estado Global] Atualizado via Camera (1Hz) frame={payload.get('frame_id')}")
            # self.latest_vision = clip_encoder(payload)
            
        elif topic == "tos.events":
            print(f"[Estado Global] Contexto TOS recebido: {payload.get('event_type')}")
        
        # O estado global Pt está sempre pronto a ser lido pelas aplicações downstream
        fused_pt = self._generate_fused_picture()
        # Aqui poderias emitir 'fused_pt' para um tópico interno do Edge Server ou DB vetorial


def safe_payload(raw_value: bytes) -> dict[str, Any]:
    return json.loads(raw_value.decode("utf-8"))

def start_perception_bridge() -> None:
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe(TOPICS)

    print("[Kafka consumer] Active. Waiting for independent streams to perform trigger-based fusion...")
    
    global_state = MultimodalGlobalState()

    try:
        while True:
            msg = consumer.poll(0.01) # Reduzido para maior responsividade (assíncrono)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[Kafka consumer] error: {msg.error()}")
                continue

            topic = msg.topic()
            payload = safe_payload(msg.value())
            
            # Delega a gestão temporal e integração para a classe de estado
            global_state.update_modality(topic, payload)

    except KeyboardInterrupt:
        print("\n[Kafka consumer] shutting down...")
    finally:
        consumer.close()

if __name__ == "__main__":
    start_perception_bridge()
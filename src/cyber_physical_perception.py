from __future__ import annotations
import json
import os
import torch
import torch.nn as nn
from typing import Any
from confluent_kafka import Consumer, KafkaError

print("🧠 [Ciber-Physical Brain] A inicializar módulo de Cross-Attention (Protótipo Arquitetural)...")

# --- PROTÓTIPO DA REDE NEURAL (Baseado na Literatura do NotebookLM) ---
class CausalCrossAttentionFusion(nn.Module):
    def __init__(self, imu_dim=6, ran_dim=6, embed_dim=16):
        super().__init__()
        # Projeções para alinhar as dimensões dos sensores no mesmo espaço latente
        self.imu_proj = nn.Linear(imu_dim, embed_dim)
        self.ran_proj = nn.Linear(ran_dim, embed_dim)
        
        # O mecanismo core sugerido pela literatura (Query = IMU, Key/Value = RAN)
        self.cross_attention = nn.MultiheadAttention(embed_dim, num_heads=2, batch_first=True)
        
    def forward(self, imu_tensor, ran_tensor):
        # 1. Projetar os dados crus
        q_imu = self.imu_proj(imu_tensor).unsqueeze(1) # Query (A física do Drone)
        k_ran = self.ran_proj(ran_tensor).unsqueeze(1) # Key (O estado do Rádio)
        v_ran = k_ran                                  # Value
        
        # 2. O cruzamento causal: Onde o Pitch se cruza com o SINR
        attn_output, attn_weights = self.cross_attention(q_imu, k_ran, v_ran)
        return attn_output.squeeze(), attn_weights

# Instanciar o modelo (Nota: Pesos estão aleatórios sem treino)
fusion_model = CausalCrossAttentionFusion()

# --- INFRAESTRUTURA KAFKA ---
KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "group.id": os.getenv("KAFKA_GROUP_ID", "cyber-physical-perception-consumer"),
    "auto.offset.reset": "earliest",
}
TOPICS = ["uav.imu", "uav.ran.noise"]

def start_perception_bridge() -> None:
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe(TOPICS)

    print("📡 [Brain] À escuta dos tópicos. A fundir IMU e RAN via Cross-Attention...")
    
    last_imu = [0.0] * 6
    last_ran = [0.0] * 6

    try:
        while True:
            msg = consumer.poll(0.1)
            if msg is None or msg.error():
                continue

            topic = msg.topic()
            payload = json.loads(msg.value().decode("utf-8"))
            
            # 1. Atualizar o Buffer com as grandezas físicas e de rede cruas
            if topic == "uav.imu":
                last_imu = [payload.get(k, 0) for k in ["altitude_m", "speed_mps", "roll_deg", "pitch_deg", "yaw_deg", "timestamp_s"]]
            elif topic == "uav.ran.noise":
                last_ran = [payload.get(k, 0) for k in ["sinr_db", "rsrp_dbm", "bler", "latency_ms", "ue_x_m", "ue_y_m"]]

            # 2. A Magia da Fusão Neural (Apenas se tivermos ambos os dados)
            if last_imu[0] != 0.0 and last_ran[0] != 0.0:
                # Substitui categoricamente qualquer None por 0.0 antes de enviar para o PyTorch
                sanitized_imu = [x if x is not None else 0.0 for x in last_imu]
                sanitized_ran = [x if x is not None else 0.0 for x in last_ran]

                t_imu = torch.tensor([sanitized_imu], dtype=torch.float32)
                t_ran = torch.tensor([sanitized_ran], dtype=torch.float32)
                
                # Executa a arquitetura Cross-Attention
                fused_latent, weights = fusion_model(t_imu, t_ran)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    start_perception_bridge()
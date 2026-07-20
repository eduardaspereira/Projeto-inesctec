import json
import numpy as np
import torch
import torch.nn as nn
import joblib
import time
import os
from collections import deque
from confluent_kafka import Consumer

# =========================================================
# 1. Configuracao da Rede Neural Preditiva (LSTM)
# =========================================================
class ObstaclePredictorLSTM(nn.Module):
    def __init__(self, input_dim=256, hidden_dim=128, num_layers=2):
        super().__init__()
        self.lstm = nn.LSTM(input_dim, hidden_dim, num_layers, batch_first=True, dropout=0.1)
        self.classifier = nn.Sequential(
            nn.Linear(hidden_dim, 64),
            nn.ReLU(),
            nn.Linear(64, 1),
            nn.Sigmoid()
        )

    def forward(self, x):
        _, (hidden, _) = self.lstm(x)
        return self.classifier(hidden[-1])

device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"[Inicializacao NC] Dispositivo de inferencia alocado: {device.upper()}")

predictor_model = ObstaclePredictorLSTM().to(device)
predictor_model.eval()

HISTORY_STEPS = 10
trajectory_buffer = deque(maxlen=HISTORY_STEPS)
pca_inversor = None  # Inicializado a vazio para nao bloquear o arranque

# =========================================================
# 2. Configuracao do Consumidor Kafka (Dual-Topic)
# =========================================================
consumer_conf = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'network_controller_group',
    'auto.offset.reset': 'latest',
    'fetch.message.max.bytes': 5242880
}
consumer = Consumer(consumer_conf)
# O NC subscreve agora ambos os topicos. Escuta o RAN e o VC simultaneamente.
consumer.subscribe(['NC_topic', 'RAN_topic'])

def dequantize_from_int8(quantized_list, scale, min_val):
    quantized_array = np.array(quantized_list, dtype=np.int8)
    dequantized = (quantized_array + 128) * scale + min_val
    return dequantized.astype(np.float32)

print("\n[Sistema NC] Network Controller Ativo. A atuar em arquitetura Multi-Tier (RAN + Percecao)...")

# Memoria de estado global
latest_ran_health = "BOM"

try:
    while True:
        # Polling ultra-rapido (100ms) para reagir quase em tempo real
        msg = consumer.poll(0.1)
        
        if msg is None: continue
        if msg.error(): continue

        topic = msg.topic()
        payload = json.loads(msg.value().decode('utf-8'))
        msg_timestamp = payload.get("timestamp", 0)

        # =========================================================
        # TIER 1: Reacao Imediata as Metricas da Rede (RAN)
        # =========================================================
        if topic == 'RAN_topic':
            # Extrai uma metrica chave, ex: Downlink Throughput (Mbps)
            dl_tput = float(payload.get("dl_tput_mbps", 0))
            
            # Logica de reacao puramente baseada em rede
            if dl_tput < 10.0:
                latest_ran_health = "DEGRADADO"
                print(f"[{msg_timestamp}s] [Controlo RAN] Throughput critico ({dl_tput} Mbps). A ajustar ganho da antena AP para compensar...")
            else:
                latest_ran_health = "BOM"

        # =========================================================
        # TIER 2: Processamento Avançado Multimodal (VC)
        # =========================================================
        elif topic == 'NC_topic':
            # Tenta carregar o PCA em tempo de execucao (on-the-fly) sem bloquear o sistema
            if pca_inversor is None:
                if os.path.exists('pca_reducer.pkl'):
                    pca_inversor = joblib.load('pca_reducer.pkl')
                    print(f"[{msg_timestamp}s] [Sistema NC] Matriz PCA recebida do Edge. Visao Multimodal destrancada.")
                else:
                    # Ignora o pacote de visao se a calibracao matematicamente exata ainda nao chegou
                    continue

            processing_start_time = time.perf_counter()
            status_flag = payload.get("status")
            
            latent_256d = dequantize_from_int8(
                payload.get("obstacle_state_vector"), 
                payload["quantization_params"]["scale"], 
                payload["quantization_params"]["min_val"]
            )
            
            trajectory_buffer.append(latent_256d)
            
            network_slice_decision = "eMBB"
            los_block_probability = 0.0

            # Fusao de Decisoes (Network + AI Perception)
            if status_flag == "CRITICAL_ANOMALY":
                network_slice_decision = "URLLC"
                print(f"[{msg_timestamp}s] [Acao Conjunta] Anomalia Cinematica do UAV. Comutacao imediata para URLLC.")
                
            elif len(trajectory_buffer) == HISTORY_STEPS:
                trajectory_tensor = torch.tensor(np.array(trajectory_buffer), dtype=torch.float32).unsqueeze(0).to(device)
                
                with torch.no_grad():
                    los_block_probability = predictor_model(trajectory_tensor).item()
                    
                # Se a visao preve bloqueio OU se a rede ja esta a dar sinais de quebra, atribui URLLC
                if los_block_probability > 0.75 or latest_ran_health == "DEGRADADO":
                    network_slice_decision = "URLLC"
                    
            processing_latency_ms = (time.perf_counter() - processing_start_time) * 1000
            
            if len(trajectory_buffer) == HISTORY_STEPS:
                print(f"[{msg_timestamp}s] [Orquestracao E2E] Prob. Bloqueio: {los_block_probability*100:.1f}% | RAN Status: {latest_ran_health} -> Slicing Final: {network_slice_decision} (Latencia CPU: {processing_latency_ms:.2f}ms)")

except KeyboardInterrupt:
    print("\n[Sistema NC] Termino da execucao pelo utilizador. A encerrar controlador...")
finally:
    consumer.close()
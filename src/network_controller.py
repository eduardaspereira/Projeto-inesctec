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
pca_inversor = None

# =========================================================
# 2. Configuracao do Consumidor Kafka
# =========================================================
consumer_conf = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'network_controller_group',
    'auto.offset.reset': 'latest',
    'fetch.message.max.bytes': 5242880
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['NC_topic'])

def dequantize_from_int8(quantized_list, scale, min_val):
    quantized_array = np.array(quantized_list, dtype=np.int8)
    dequantized = (quantized_array + 128) * scale + min_val
    return dequantized.astype(np.float32)

print("\n[Sistema NC] Network Controller Ativo. A atuar em arquitetura Multi-Tier (RAN + Percecao)...")

try:
    while True:
        msg = consumer.poll(0.1)
        
        if msg is None: continue
        if msg.error(): continue

        payload = json.loads(msg.value().decode('utf-8'))
        msg_timestamp = payload.get("timestamp", 0)

        # =========================================================
        # TIER 2: Processamento Avançado Multimodal (VC)
        # =========================================================
        if pca_inversor is None:
            if os.path.exists(os.path.abspath('pca_reducer.pkl')):
                pca_inversor = joblib.load(os.path.abspath('pca_reducer.pkl'))
                print(f"[{msg_timestamp}s] [Sistema NC] Matriz PCA recebida do Edge. Visao Multimodal destrancada.")
            else:
                continue

        processing_start_time = time.perf_counter()
        
        # Extrair dados do payload
        status_flag = payload.get("status")
        agent_insights = payload.get("agent_insights", {})
        connection_estimate = agent_insights.get("connection_estimate", "LESS_CRITICAL")
        
        # Extracao Segura (Tolerancia a Falhas para pacotes exclusivamente RAN)
        obs_vector = payload.get("obstacle_state_vector")
        quant_params = payload.get("quantization_params")

        if obs_vector is not None and quant_params is not None:
            latent_256d = dequantize_from_int8(
                obs_vector, 
                quant_params.get("scale", 1.0), 
                quant_params.get("min_val", 0.0)
            )
            trajectory_buffer.append(latent_256d)
        else:
            # Se for um pacote apenas de rede, ignoramos a inferencia espacial neste milissegundo
            pass
        
        network_slice_decision = "eMBB" # Default
        los_block_probability = 0.0

        # Fusao de Decisoes (Network + AI Perception)
        if status_flag == "CRITICAL_ANOMALY":
            network_slice_decision = "URLLC"
            print(f"[{msg_timestamp}s] [Acao Conjunta] Anomalia Cinematica do UAV. Comutacao imediata para URLLC.")
            
        elif len(trajectory_buffer) == HISTORY_STEPS:
            trajectory_tensor = torch.tensor(np.array(trajectory_buffer), dtype=torch.float32).unsqueeze(0).to(device)
            
            with torch.no_grad():
                los_block_probability = predictor_model(trajectory_tensor).item()
                
            # Logica atualizada: Se a rede neural prever bloqueio OU a Flag do Agente for CRITICA
            if los_block_probability > 0.75 or connection_estimate == "CRITICAL":
                network_slice_decision = "URLLC"
                
        processing_latency_ms = (time.perf_counter() - processing_start_time) * 1000
        
        if len(trajectory_buffer) == HISTORY_STEPS:
            # Imprime a decisao final e a razao principal (se foi o Agente de Imagem a forcar a mudanca)
            print(f"[{msg_timestamp}s] [Orquestracao E2E] Agente Visual: {connection_estimate} | RAN -> Slicing: {network_slice_decision} (Latencia NC: {processing_latency_ms:.2f}ms)")

except KeyboardInterrupt:
    print("\n[Sistema NC] Termino da execucao pelo utilizador. A encerrar controlador...")
finally:
    consumer.close()
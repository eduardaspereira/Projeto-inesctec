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
        ran_metrics = payload.get("ran_metrics")
        status_flag = payload.get("status")
        agent_insights = payload.get("agent_insights")
        
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

        # =========================================================
        # FASE 1: Otimização baseada apenas em KPM (RAN)
        # Segue o diagrama: "Ordem de otimização (baseada apenas em KPM)"
        # =========================================================
        ran_slice_decision = "eMBB"
        if ran_metrics:
            try:
                # Se o SINR for baixo ou o BLER for alto, a rede física está instável
                sinr = float(ran_metrics.get("sinr_db", 20))
                bler = float(ran_metrics.get("bler", 0))
                if sinr < 12.0 or bler > 0.1:
                    ran_slice_decision = "URLLC"
            except (ValueError, TypeError):
                pass
            
            # Reporta a decisão inicial (se for apenas RAN, inclui latência)
            if not agent_insights:
                processing_latency_ms = (time.perf_counter() - processing_start_time) * 1000
                print(f"[{msg_timestamp}s] [Conexão Otimizada (1ª versão)] Baseada apenas em KPM -> Slicing: {ran_slice_decision} (Latência: {processing_latency_ms:.2f}ms)")
            else:
                print(f"[{msg_timestamp}s] [Conexão Otimizada (1ª versão)] Baseada apenas em KPM -> Slicing: {ran_slice_decision}")

        # =========================================================
        # FASE 2: Re-otimização (Prioridade ao vc_info)
        # Segue o diagrama: "Dá prioridade a 'vc_info' sobre KPM"
        # =========================================================
        if agent_insights:
            connection_estimate = agent_insights.get("connection_estimate", "LESS_CRITICAL")
            final_slice_decision = ran_slice_decision
            los_block_probability = 0.0
            reason = "KPM Estável" if final_slice_decision == "eMBB" else "RAN Degradada"

            # 1. Prioridade Máxima: Conclusões do VC (Visão/LLM)
            if connection_estimate == "CRITICAL":
                final_slice_decision = "URLLC"
                reason = "VC Insight (Aviso de Obstrução Crítica)"
            
            # 2. Prioridade Secundária: Anomalias detetadas no VC (IMU/TOS)
            elif status_flag == "CRITICAL_ANOMALY":
                final_slice_decision = "URLLC"
                reason = "Anomalia Cinemática detetada pelo VC"
                
            # 3. Apoio à Decisão: Predição LSTM do NC (KPM Espacial)
            elif len(trajectory_buffer) == HISTORY_STEPS:
                trajectory_tensor = torch.tensor(np.array(trajectory_buffer), dtype=torch.float32).unsqueeze(0).to(device)
                with torch.no_grad():
                    los_block_probability = predictor_model(trajectory_tensor).item()
                
                if los_block_probability > 0.75:
                    final_slice_decision = "URLLC"
                    reason = f"Predição de Bloqueio NC ({los_block_probability:.2f})"

            processing_latency_ms = (time.perf_counter() - processing_start_time) * 1000
            
            # Imprime a decisão final com prioridade ao vc_info
            print(f"[{msg_timestamp}s] [Conexão Reotimizada (final)] Prioridade ao vc_info -> Slicing: {final_slice_decision} | Razão: {reason} (Latência: {processing_latency_ms:.2f}ms)")

except KeyboardInterrupt:
    print("\n[Sistema NC] Termino da execucao pelo utilizador. A encerrar controlador...")
finally:
    consumer.close()
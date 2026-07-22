import json
import numpy as np
import time
import torch
import torch.nn as nn
from confluent_kafka import Consumer
from collections import deque

# =========================================================
# 1. Arquitetura da Rede (O teu modelo Multi-Label Dinâmico)
# =========================================================
class NetworkSlicingLSTM(nn.Module):
    def __init__(self, input_dim=256, hidden_dim=128, num_layers=2, max_ues=15):
        super().__init__()
        self.lstm = nn.LSTM(input_dim, hidden_dim, num_layers, batch_first=True, dropout=0.1)
        self.dropout = nn.Dropout(0.2)
        self.fc = nn.Linear(hidden_dim, max_ues)
        
    def forward(self, x):
        out, _ = self.lstm(x)
        out = out[:, -1, :] 
        out = self.dropout(out)
        out = self.fc(out)
        return torch.sigmoid(out)

# =========================================================
# 2. Configuração do Sistema
# =========================================================
device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"[Inicializacao NC] Dispositivo de inferencia alocado: {device.upper()}")

MAX_SUPPORTED_UES = 15
model = NetworkSlicingLSTM(max_ues=MAX_SUPPORTED_UES).to(device)
model.eval()

SEQ_LENGTH = 15
time_window = deque(maxlen=SEQ_LENGTH)
THREAT_THRESHOLD = 0.75 

dynamic_ue_registry = {}

# =========================================================
# 3. Consumidor Kafka (Leitura do Edge Gateway)
# =========================================================
consumer_conf = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'network_controller_group',
    'auto.offset.reset': 'latest',
    'fetch.message.max.bytes': 5242880
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['NC_topic'])

def dequantize_vector(quantized_list, scale, min_val):
    arr = np.array(quantized_list, dtype=np.float32)
    return (arr + 128) * scale + min_val

print(f"\n[Sistema NC] Arquitetura Multi-Tier Ativa. Suporta até {MAX_SUPPORTED_UES} UEs.")

try:
    while True:
        msg = consumer.poll(0.1)
        if msg is None: continue
        if msg.error(): continue

        processing_start_time = time.perf_counter()

        payload = json.loads(msg.value().decode('utf-8'))
        timestamp = payload.get("timestamp", 0)
        
        ran_metrics = payload.get("ran_metrics")
        status_flag = payload.get("status")
        agent_insights = payload.get("agent_insights", {})
        
        quantized_vector = payload.get("obstacle_state_vector")
        q_params = payload.get("quantization_params", {})

        edge_affected_ue = agent_insights.get("affected_ue")
        connection_estimate = agent_insights.get("connection_estimate", "LESS_CRITICAL")

        # ---------------------------------------------------------
        # Lógica de Auto-Descoberta (A tua implementação)
        # ---------------------------------------------------------
        if edge_affected_ue and edge_affected_ue not in dynamic_ue_registry.values():
            new_idx = len(dynamic_ue_registry)
            if new_idx < MAX_SUPPORTED_UES:
                dynamic_ue_registry[new_idx] = edge_affected_ue
                print(f"[{timestamp}s] [Sistema] Novo equipamento detetado: {edge_affected_ue} (Índice {new_idx})")

        # ---------------------------------------------------------
        # FASE 1: Otimização baseada apenas em KPM (Do teu colega)
        # ---------------------------------------------------------
        ran_slice_decision = "eMBB"
        if ran_metrics:
            try:
                sinr = float(ran_metrics.get("sinr_db", 20))
                bler = float(ran_metrics.get("bler", 0))
                if sinr < 12.0 or bler > 0.1:
                    ran_slice_decision = "URLLC"
            except (ValueError, TypeError):
                pass

        # ---------------------------------------------------------
        # FASE 2: Re-otimização Multimodal e Preditiva
        # ---------------------------------------------------------
        final_slice_decision = ran_slice_decision
        reason = "KPM Estável" if final_slice_decision == "eMBB" else "RAN Degradada (SINR/BLER)"
        
        # 1. Prioridade Máxima: Anomalia Cinemática Crítica
        if status_flag == "CRITICAL_ANOMALY":
            final_slice_decision = "URLLC"
            reason = "Anomalia Cinemática detetada pelo VC"
            
        # 2. Prioridade Máxima: Câmara deteta perigo imediato
        elif connection_estimate == "CRITICAL":
            final_slice_decision = "URLLC"
            reason = f"Aviso Reativo da Visão (Risco para {edge_affected_ue})"
            
        # 3. Apoio à Decisão: Predição LSTM Multi-UE
        elif quantized_vector and q_params:
            scale = q_params.get("scale", 1.0)
            min_val = q_params.get("min_val", 0.0)
            real_vector = dequantize_vector(quantized_vector, scale, min_val)
            time_window.append(real_vector)
            
            if len(time_window) == SEQ_LENGTH:
                seq_tensor = torch.tensor(np.array(time_window), dtype=torch.float32).unsqueeze(0).to(device)
                
                with torch.no_grad():
                    risk_scores = model(seq_tensor)[0].cpu().numpy()
                
                multiple_threats = []
                for idx, ue_name in dynamic_ue_registry.items():
                    ue_risk = risk_scores[idx]
                    if ue_risk >= THREAT_THRESHOLD:
                        multiple_threats.append(f"{ue_name} ({ue_risk*100:.1f}%)")
                
                if multiple_threats:
                    final_slice_decision = "URLLC"
                    reason = f"Predição LSTM: Risco futuro para {', '.join(multiple_threats)}"

        # ---------------------------------------------------------
        # Finalização e Registo de Latência
        # ---------------------------------------------------------
        processing_latency_ms = (time.perf_counter() - processing_start_time) * 1000
        
        # Só imprime se houver efetivamente uma ação ou se estivermos em debug
        if final_slice_decision == "URLLC":
            print(f"[{timestamp}s] ⚡ AÇÃO NC: Slicing -> {final_slice_decision} | Razão: {reason} (Latência NC: {processing_latency_ms:.2f}ms)")

except KeyboardInterrupt:
    print("\n[Sistema NC] Encerramento solicitado pelo utilizador.")
finally:
    consumer.close()
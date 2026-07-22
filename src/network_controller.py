import json
import numpy as np
import torch
import torch.nn as nn
from confluent_kafka import Consumer
from collections import deque

# =========================================================
# 1. Arquitetura da Rede (Multi-Label Dinâmica)
# =========================================================
class NetworkSlicingLSTM(nn.Module):
    # max_ues define o limite máximo de equipamentos que a rede suporta de raiz (ex: 15)
    def __init__(self, input_dim=256, hidden_dim=128, num_layers=2, max_ues=15):
        super().__init__()
        self.lstm = nn.LSTM(input_dim, hidden_dim, num_layers, batch_first=True)
        self.dropout = nn.Dropout(0.2)
        
        # output genérico para até max_ues
        self.fc = nn.Linear(hidden_dim, max_ues)
        
    def forward(self, x):
        out, _ = self.lstm(x)
        out = out[:, -1, :] 
        out = self.dropout(out)
        out = self.fc(out)
        
        # Sigmoid avalia cada UE de 0.0 a 1.0 independentemente!
        return torch.sigmoid(out)

# =========================================================
# 2. Configuração do Sistema
# =========================================================
device = "cuda" if torch.cuda.is_available() else "cpu"
MAX_SUPPORTED_UES = 15
model = NetworkSlicingLSTM(max_ues=MAX_SUPPORTED_UES).to(device)
model.eval()

SEQ_LENGTH = 15
time_window = deque(maxlen=SEQ_LENGTH)
THREAT_THRESHOLD = 0.75 

# Registo dinâmico de UEs 
dynamic_ue_registry = {}

# =========================================================
# 3. Consumidor Kafka (Leitura do Edge Gateway)
# =========================================================
consumer = Consumer({
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'network_controller_group',
    'auto.offset.reset': 'latest'
})
consumer.subscribe(['NC_topic'])

def dequantize_vector(quantized_list, scale, min_val):
    arr = np.array(quantized_list, dtype=np.float32)
    return (arr + 128) * scale + min_val

print(f"\n[NC] Sistema escalável iniciado. Suporta até {MAX_SUPPORTED_UES} UEs em simultâneo.")
print("[NC] A escutar fluxo multimodal no 'NC_topic'...")

try:
    while True:
        msg = consumer.poll(0.1)
        if msg is None: continue
        if msg.error(): continue

        payload = json.loads(msg.value().decode('utf-8'))
        timestamp = payload.get("timestamp")
        quantized_vector = payload.get("obstacle_state_vector")
        q_params = payload.get("quantization_params", {})
        
        # O Edge diz-nos qual foi o UE afetado pela visão para podermos registá-lo dinamicamente
        edge_affected_ue = payload.get("agent_insights", {}).get("affected_ue")
        is_vision_critical = payload.get("agent_insights", {}).get("connection_estimate") == "CRITICAL"

        # ---------------------------------------------------------
        # Lógica de Auto-Descoberta (Sem Hardcode)
        # ---------------------------------------------------------
        if edge_affected_ue and edge_affected_ue not in dynamic_ue_registry.values():
            new_idx = len(dynamic_ue_registry)
            if new_idx < MAX_SUPPORTED_UES:
                dynamic_ue_registry[new_idx] = edge_affected_ue
                print(f"[Sistema] Novo equipamento detetado e mapeado na rede: {edge_affected_ue} (Índice {new_idx})")

        # ---------------------------------------------------------
        # Processamento e Previsão
        # ---------------------------------------------------------
        if quantized_vector and q_params:
            scale = q_params.get("scale", 1.0)
            min_val = q_params.get("min_val", 0.0)
            real_vector = dequantize_vector(quantized_vector, scale, min_val)
            time_window.append(real_vector)
            
            if len(time_window) == SEQ_LENGTH:
                seq_tensor = torch.tensor(np.array(time_window), dtype=torch.float32).unsqueeze(0).to(device)
                
                with torch.no_grad():
                    # Devolve um array com 'MAX_SUPPORTED_UES' probabilidades independentes
                    risk_scores = model(seq_tensor)[0].cpu().numpy()
                
                # Avaliar todos os UEs registados simultaneamente
                multiple_threats_detected = False
                
                for idx, ue_name in dynamic_ue_registry.items():
                    ue_risk = risk_scores[idx]
                    
                    if ue_risk >= THREAT_THRESHOLD:
                        multiple_threats_detected = True
                        print(f"[{timestamp}s] PREDITIVO: Risco de {ue_risk*100:.1f}% para o {ue_name}! A alocar Slice URLLC para este UE...")

                if is_vision_critical and not multiple_threats_detected:
                     print(f"[{timestamp}s] REATIVO: Visão detetou perigo iminente no {edge_affected_ue}. A forçar failover!")

except KeyboardInterrupt:
    print("\n[NC] Encerramento solicitado pelo utilizador.")
finally:
    consumer.close()
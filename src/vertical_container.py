import json
import numpy as np
import torch
import os
import joblib
import torch.nn as nn
from PIL import Image
from transformers import CLIPProcessor, CLIPModel
from sentence_transformers import SentenceTransformer
from sklearn.decomposition import PCA
from confluent_kafka import Consumer, Producer
from collections import deque

# =========================================================
# 1. Configuracao de Hardware e Modelos
# =========================================================
device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"[Inicializacao] Vertical Container alocado no dispositivo: {device.upper()}")

print("[Inicializacao] A carregar modelo de Visao (CLIP)...")
clip_model = CLIPModel.from_pretrained("openai/clip-vit-base-patch32").to(device)
clip_processor = CLIPProcessor.from_pretrained("openai/clip-vit-base-patch32")

print("[Inicializacao] A carregar modelo de Processamento de Linguagem Natural (MiniLM)...")
nlp_model = SentenceTransformer('all-MiniLM-L6-v2', device=device)
nlp_model.encode("warmup") 

torch.manual_seed(42) 
nlp_proj = nn.Linear(384, 128).to(device)
nlp_proj.eval()

print("[Inicializacao] A carregar modelo do IMU (Autoencoder)...")
class PreLNResidualBlock(nn.Module):
    def __init__(self, dim):
        super().__init__()
        self.ln = nn.LayerNorm(dim)
        self.linear1 = nn.Linear(dim, dim)
        self.gelu = nn.GELU()
        self.linear2 = nn.Linear(dim, dim)
        self.dropout = nn.Dropout(0.1)

    def forward(self, x):
        residual = x
        out = self.ln(x)
        out = self.gelu(self.linear1(out))
        out = self.dropout(self.linear2(out))
        return residual + out

imu_encoder = nn.Sequential(
    nn.Linear(12, 128),
    PreLNResidualBlock(128),
    PreLNResidualBlock(128)
).to(device)

try:
    imu_encoder.load_state_dict(torch.load('imu_encoder.pth', map_location=device, weights_only=True))
    imu_encoder.eval()
    
    imu_norm = np.load('imu_normalization.npz')
    imu_mean = imu_norm['mean']
    imu_std = imu_norm['std']
except FileNotFoundError:
    print("[Erro] Ficheiros 'imu_encoder.pth' ou 'imu_normalization.npz' ausentes do diretorio.")
    exit(1)
except RuntimeError as e:
    print(f"[Erro] Incompatibilidade arquitetural nos pesos do IMU. Detalhe: {e}")
    exit(1)

# Configuracao Temporal e Dimensional
WINDOW_SIZE = 50
imu_buffer = deque(maxlen=WINDOW_SIZE)
last_timestamp = None

# Configuracao PCA Dinamica (Calibracao Baseada em Dados Reais)
pca_reducer = PCA(n_components=256)
pca_is_fitted = False
calibration_buffer = []
CALIBRATION_SAMPLES = 300

# =========================================================
# 2. Funcoes de Extracao e Processamento
# =========================================================
def quantize_to_int8(tensor):
    min_val, max_val = tensor.min(), tensor.max()
    scale = (max_val - min_val) / 255.0
    if scale == 0:
        return np.zeros(tensor.shape, dtype=np.int8), 0.0, float(min_val)
    quantized = np.round((tensor - min_val) / scale) - 128
    return quantized.astype(np.int8), float(scale), float(min_val)

def extract_image_embedding(image_path):
    if image_path is None or str(image_path) == "None" or str(image_path).strip() == "":
        return np.zeros(512)
        
    try:
        image = Image.open(image_path)
        inputs = clip_processor(images=image, return_tensors="pt").to(device)
        
        with torch.no_grad():
            features = clip_model.get_image_features(pixel_values=inputs['pixel_values'])
            if hasattr(features, 'pooler_output'):
                emb = features.pooler_output
            elif hasattr(features, 'image_embeds'):
                emb = features.image_embeds
            else:
                emb = features
                
        return emb.cpu().numpy().flatten()
        
    except Exception as e:
        print(f"[Aviso] Falha na extracao visual ({image_path}): {e}")
        return np.zeros(512)

def extract_nlp_embedding(tos_data):
    if not tos_data:
        return np.zeros(128)
    
    text_event = f"{tos_data.get('event_type', '')}, {tos_data.get('entity_id', '')}, {tos_data.get('entity_type', '')}, {tos_data.get('terminal', '')}"
    
    with torch.no_grad():
        emb_384 = nlp_model.encode(text_event, convert_to_tensor=True).to(device)
        emb_128 = nlp_proj(emb_384)
    return emb_128.cpu().numpy()

def process_imu_window(buffer):
    if len(buffer) < WINDOW_SIZE:
        return np.zeros(128), False
    
    acc_x = np.array([float(row.get('acc_x', 0)) for row in buffer])
    acc_y = np.array([float(row.get('acc_y', 0)) for row in buffer])
    acc_z = np.array([float(row.get('acc_z', 0)) for row in buffer])
    gyro_roll = np.array([float(row.get('gyro_roll', 0)) for row in buffer])
    altitude = np.array([float(row.get('altitude_m', 0)) for row in buffer])

    # Detecao Local de Anomalias (Flag CRITICAL_ANOMALY)
    # Limiar arbitrario para a literatura, avaliando variancia severa em aceleracao Z (ex: queda livre ou impacto)
    acc_z_std = float(np.std(acc_z))
    is_anomalous = acc_z_std > 3.0 

    features = [
        float(np.mean(acc_x)), float(np.mean(acc_y)), float(np.mean(acc_z)),
        float(np.std(acc_x)), float(np.std(acc_y)), acc_z_std,
        float(np.mean(gyro_roll)), 0.0, 0.0, 
        float(np.min(altitude)), float(np.max(altitude)), float(np.mean(altitude))
    ]
    
    X = np.array(features, dtype=np.float32)
    X_norm = (X - imu_mean) / (imu_std + 1e-8)
    
    tensor_features = torch.tensor(X_norm, dtype=torch.float32).to(device)
    with torch.no_grad():
        imu_emb = imu_encoder(tensor_features)
        
    return imu_emb.cpu().numpy(), is_anomalous

# =========================================================
# 3. Configuracao Kafka e Ciclo Principal
# =========================================================
consumer = Consumer({
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'vc_perception_group',
    'auto.offset.reset': 'latest',
    'fetch.message.max.bytes': 5242880
})
consumer.subscribe(['VC_topic'])

producer = Producer({
    'bootstrap.servers': 'localhost:29092',
    'message.max.bytes': 5242880
})

def delivery_report(err, msg):
    if err:
        print(f"[Erro] Falha de entrega Kafka: {err}")

print("\n[Sistema] Vertical Container em execucao. A aguardar fluxo de dados no topico 'VC_topic'...")

try:
    while True:
        msg = consumer.poll(0.5)
        if msg is None: continue
        if msg.error(): continue

        payload = json.loads(msg.value().decode('utf-8'))
        timestamp = payload.get("timestamp")
        
        # Gestao de Falhas - Limpeza de Buffer em caso de interrupcao do sinal
        if last_timestamp is not None and (timestamp - last_timestamp) > 1.0:
            imu_buffer.clear()
            print(f"[{timestamp}s] [Aviso] Descontinuidade temporal detetada. Buffer cinemático limpo.")
        last_timestamp = timestamp

        imu_data_raw = payload.get("imu_data")
        if imu_data_raw:
            imu_buffer.append(imu_data_raw)
            
        img_emb = extract_image_embedding(payload.get("image_frame"))
        nlp_emb = extract_nlp_embedding(payload.get("tos_data"))
        imu_emb, is_anomalous = process_imu_window(imu_buffer)

        if payload.get("image_frame") or payload.get("tos_data"):
            print(f"[{timestamp}s] [Processamento] Agregacao Multimodal Ativa.")
            
            fused_vector = np.concatenate((img_emb, imu_emb, nlp_emb))
            
            # Calibracao de Base Real do PCA (Eliminacao de Mock Data)
            if not pca_is_fitted:
                calibration_buffer.append(fused_vector)
                if len(calibration_buffer) >= CALIBRATION_SAMPLES:
                    pca_reducer.fit(calibration_buffer)
                    pca_is_fitted = True
                    joblib.dump(pca_reducer, 'pca_reducer.pkl')
                    print("[Sistema] Matriz PCA calibrada de forma autonoma com dados reais do terreno.")
                else:
                    # Nao emite output enquanto o PCA nao estiver fiavelmente calibrado
                    continue

            reduced_vector = pca_reducer.transform([fused_vector])[0]
            quantized_vector, scale, min_val = quantize_to_int8(reduced_vector)

            # Exportacao Continua para Auditoria Visual (TensorBoard)
            with open("tensors.tsv", "a") as f_vec:
                f_vec.write("\t".join(map(str, reduced_vector)) + "\n")
            
            tos_event = payload.get("tos_data", {}).get("event_type", "NOMINAL") if payload.get("tos_data") else "NOMINAL"
            
            if not os.path.exists("metadata.tsv") or os.stat("metadata.tsv").st_size == 0:
                with open("metadata.tsv", "w") as f_meta:
                    f_meta.write("Timestamp\tEvento_TOS\n")
            
            with open("metadata.tsv", "a") as f_meta:
                f_meta.write(f"{timestamp}\t{tos_event}\n")
            
            # Avaliacao Previa de Estado (Aviso SOTA)
            current_status = "CRITICAL_ANOMALY" if is_anomalous else "TRACKING_ACTIVE"
            if current_status == "CRITICAL_ANOMALY":
                print(f"[{timestamp}s] [Alerta] Anomalia Cinematica Severa Detetada na Fase de Extracao.")

            nc_payload = {
                "timestamp": timestamp,
                "obstacle_state_vector": quantized_vector.tolist(),
                "quantization_params": {"scale": scale, "min_val": min_val},
                "status": current_status
            }
            producer.produce('NC_topic', key=str(timestamp), value=json.dumps(nc_payload), callback=delivery_report)
            producer.poll(0)

except KeyboardInterrupt:
    print("\n[Sistema] Termino da execucao pelo utilizador. A encerrar instancias...")
finally:
    consumer.close()
    producer.flush()
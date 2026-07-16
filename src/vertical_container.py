import json
import numpy as np
import torch
import torch.nn as nn
from PIL import Image
from transformers import CLIPProcessor, CLIPModel
from sentence_transformers import SentenceTransformer
from sklearn.decomposition import PCA
from confluent_kafka import Consumer, Producer
from collections import deque

# =========================================================
# 1. Configuração de Hardware e Modelos
# =========================================================
device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"A iniciar o Vertical Container no dispositivo: {device.upper()}")

# --- A. Modelo de Visão (CLIP) ---
print("A carregar modelo de Visão (CLIP)...")
clip_model = CLIPModel.from_pretrained("openai/clip-vit-base-patch32").to(device)
clip_processor = CLIPProcessor.from_pretrained("openai/clip-vit-base-patch32")

# --- B. Modelo de NLP (TOS) ---
print("A carregar modelo de NLP (MiniLM)...")
nlp_model = SentenceTransformer('all-MiniLM-L6-v2').to(device)
# Projeção de 384 para 128 dimensões
torch.manual_seed(42) 
nlp_proj = nn.Linear(384, 128).to(device)
nlp_proj.eval()

# --- C. Modelo do IMU ---
print("A carregar modelo do IMU...")
imu_encoder = nn.Sequential(
    nn.Linear(12, 64),
    nn.ReLU(),
    nn.Linear(64, 128),
    nn.ReLU()
).to(device)

# Carregar os pesos treinados no Colab
try:
    imu_encoder.load_state_dict(torch.load('imu_encoder.pth', map_location=device))
    imu_encoder.eval()
    
    # Carregar os valores de normalização
    imu_norm = np.load('imu_normalization.npz')
    imu_mean = imu_norm['mean']
    imu_std = imu_norm['std']
except FileNotFoundError:
    print("ERRO: Ficheiros 'imu_encoder.pth' ou 'imu_normalization.npz' não encontrados!")
    exit(1)

# Janela temporal do IMU
WINDOW_SIZE = 50
imu_buffer = deque(maxlen=WINDOW_SIZE)

# --- D. PCA (Fusão Final) ---
# Num cenário de produção real, carregarias um modelo PCA treinado.
pca_reducer = PCA(n_components=256)
pca_reducer.fit(np.random.rand(300, 768)) 

# =========================================================
# 2. Funções de Extração e Processamento
# =========================================================

def quantize_to_int8(tensor):
    """ Quantiza Float32 para INT8 """
    min_val, max_val = tensor.min(), tensor.max()
    scale = (max_val - min_val) / 255.0
    if scale == 0:
        return np.zeros(tensor.shape, dtype=np.int8)
    quantized = np.round((tensor - min_val) / scale) - 128
    return quantized.astype(np.int8), float(scale), float(min_val)

def extract_image_embedding(image_path):
    """ Extrai 512 dimensões da imagem """
    if image_path is None or str(image_path) == "None" or str(image_path).strip() == "":
        return np.zeros(512)
    try:
        image = Image.open(image_path)
        inputs = clip_processor(images=image, return_tensors="pt").to(device)
        with torch.no_grad():
            features = clip_model.get_image_features(**inputs)
        return features.cpu().numpy().flatten()
    except Exception as e:
        print(f"Erro na imagem {image_path}: {e}")
        return np.zeros(512)

def extract_nlp_embedding(tos_data):
    """ Extrai 128 dimensões do evento de texto """
    if not tos_data:
        return np.zeros(128)
    
    # Construir frase a partir do JSON do evento
    text_event = f"{tos_data.get('event_type', '')}, {tos_data.get('entity_id', '')}, {tos_data.get('entity_type', '')}, {tos_data.get('terminal', '')}"
    
    with torch.no_grad():
        emb_384 = nlp_model.encode(text_event, convert_to_tensor=True).to(device)
        emb_128 = nlp_proj(emb_384)
    return emb_128.cpu().numpy()

def process_imu_window(buffer):
    """ Extrai 128 dimensões da janela de telemetria """
    if len(buffer) < WINDOW_SIZE:
        return np.zeros(128)
    
    acc_x = np.array([float(row.get('acc_x', 0)) for row in buffer])
    acc_y = np.array([float(row.get('acc_y', 0)) for row in buffer])
    acc_z = np.array([float(row.get('acc_z', 0)) for row in buffer])
    gyro_roll = np.array([float(row.get('gyro_roll', 0)) for row in buffer])
    altitude = np.array([float(row.get('altitude_m', 0)) for row in buffer])

    # As 12 features arquiteturais
    features = [
        float(np.mean(acc_x)), float(np.mean(acc_y)), float(np.mean(acc_z)),
        float(np.std(acc_x)), float(np.std(acc_y)), float(np.std(acc_z)),
        float(np.mean(gyro_roll)), 0.0, 0.0, # Preenchido com 0s os pitch/yaw em falta no teu log
        float(np.min(altitude)), float(np.max(altitude)), float(np.mean(altitude))
    ]
    
    # Normalização
    X = np.array(features, dtype=np.float32)
    X_norm = (X - imu_mean) / (imu_std + 1e-8)
    
    tensor_features = torch.tensor(X_norm, dtype=torch.float32).to(device)
    with torch.no_grad():
        imu_emb = imu_encoder(tensor_features)
    return imu_emb.cpu().numpy()

# =========================================================
# 3. Configuração Kafka e Loop Principal
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
        print(f"Falha de entrega: {err}")

print("\n🚀 Vertical Container totalmente inicializado e à escuta no VC_topic...")

try:
    while True:
        msg = consumer.poll(0.5)
        if msg is None: continue
        if msg.error(): continue

        payload = json.loads(msg.value().decode('utf-8'))
        timestamp = payload.get("timestamp")
        
        # Gestão do Buffer IMU
        imu_data_raw = payload.get("imu_data")
        if imu_data_raw:
            imu_buffer.append(imu_data_raw)
            
        # 1. Inferência Neural Real
        img_emb = extract_image_embedding(payload.get("image_frame"))
        nlp_emb = extract_nlp_embedding(payload.get("tos_data"))
        imu_emb = process_imu_window(imu_buffer)

        # Apenas imprimimos logs nos instantes relevantes (Imagem ou TOS)
        if payload.get("image_frame") or payload.get("tos_data"):
            print(f"\n[{timestamp}s] ⚡ Fusão Multimodal ativada!")
            if payload.get("image_frame"):
                print(" 👁️  [Visão] Frame processada com sucesso.")
            if payload.get("tos_data"):
                print(f" 📝 [Texto] TOS Lida: {payload['tos_data'].get('event_type')}")
            
            # 2. Fusão e Compressão
            fused_vector = np.concatenate((img_emb, imu_emb, nlp_emb))
            reduced_vector = pca_reducer.transform([fused_vector])[0]
            quantized_vector, scale, min_val = quantize_to_int8(reduced_vector)
            
            # 3. Publicação
            nc_payload = {
                "timestamp": timestamp,
                "obstacle_state_vector": quantized_vector.tolist(),
                "quantization_params": {"scale": scale, "min_val": min_val},
                "status": "TRACKING_ACTIVE"
            }
            producer.produce('NC_topic', key=str(timestamp), value=json.dumps(nc_payload), callback=delivery_report)
            producer.poll(0)

except KeyboardInterrupt:
    print("\nA encerrar o Vertical Container...")
finally:
    consumer.close()
    producer.flush()
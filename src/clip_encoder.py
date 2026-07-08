import os
import json
import torch
import clip
from PIL import Image
from confluent_kafka import Consumer, KafkaError

print("🧠 [CLIP Agent] A carregar modelo ViT-B/32 na CPU...")
device = "cpu"
model, preprocess = clip.load("ViT-B/32", device=device)

# --- A MÁGICA DO ZERO-SHOT (O que queremos detetar?) ---
text_descriptions = [
    "a clear line of sight, open sky and clear landscape",  # Cenário ideal
    "a physical container or large obstacle blocking the view", # Cenário NLOS (Bloqueio)
    "the ground very close, indicating a severe drone tilt or fall" # Cenário falha mecânica
]
# Tokenizar o texto para o formato do CLIP
text_tokens = clip.tokenize(text_descriptions).to(device)

print("✅ [CLIP Agent] Modelo e Prompts carregados com sucesso!")

KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "group.id": "vertical-clip-perception-group",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(KAFKA_CONFIG)
consumer.subscribe(["uav.camera"])
IMAGES_BASE_DIR = os.getenv("IMAGES_DIR", "/app/datasets/raw/uav_images/images")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue

        payload = json.loads(msg.value().decode("utf-8"))
        filename = payload.get("filename")
        frame_id = payload.get("frame_id")
        image_path = os.path.join(IMAGES_BASE_DIR, filename)
        
        if not os.path.exists(image_path):
            continue

        try:
            image = preprocess(Image.open(image_path)).unsqueeze(0).to(device)
            
            with torch.no_grad():
                # Extrair vetores da Imagem e do Texto
                image_features = model.encode_image(image)
                text_features = model.encode_text(text_tokens)
                
                # Calcular a semelhança (Cosine Similarity) e converter para probabilidades (Softmax)
                logits_per_image, logits_per_text = model(image, text_tokens)
                probs = logits_per_image.softmax(dim=-1).cpu().numpy()
            
            print(f"\n📸 [FRAME {frame_id}]: Análise Zero-Shot CLIP")
            print(f"  -> Prob. Linha de Vista (LOS): {probs[0][0]*100:.2f}%")
            print(f"  -> Prob. Obstrução (NLOS):     {probs[0][1]*100:.2f}%")
            print(f"  -> Prob. Inclinação Extrema:   {probs[0][2]*100:.2f}%")
            
            # TODO futuro: Enviar estas probabilidades de volta para o Kafka para o Consumidor Central ler!

        except Exception as e:
            print(f"❌ Erro na imagem {filename}: {e}")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
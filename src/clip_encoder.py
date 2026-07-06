import json
import os
import torch
import numpy as np
from PIL import Image
from transformers import CLIPProcessor, CLIPModel
from confluent_kafka import Consumer, Producer, KafkaError

# Configurações do Kafka e Caminhos
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
IMAGES_DIR = os.getenv("IMAGES_DIR", "/app/datasets/raw/uav_images/images")
CONSUMER_GROUP = "vision-encoder-group"
TOPIC_IN = "uav.camera"
TOPIC_OUT = "uav.embeddings.vision"

def safe_payload(raw_value: bytes) -> dict:
    return json.loads(raw_value.decode("utf-8"))

def setup_kafka():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest"
    })
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})
    return consumer, producer

def main():
    print("[CLIP Encoder] A carregar o modelo ViT-B/32 da Hugging Face...")
    
    # Deteta se há GPU disponível para acelerar a inferência
    device = "cuda" if torch.cuda.is_available() else "cpu"
    
    # Carrega o modelo pré-treinado
    model = CLIPModel.from_pretrained("openai/clip-vit-base-patch32").to(device)
    processor = CLIPProcessor.from_pretrained("openai/clip-vit-base-patch32")
    
    print(f"[CLIP Encoder] Modelo carregado com sucesso no dispositivo: {device.upper()}")

    consumer, producer = setup_kafka()
    consumer.subscribe([TOPIC_IN])

    print(f"[CLIP Encoder] À escuta de novos frames no tópico '{TOPIC_IN}'...")

    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Erro no Kafka: {msg.error()}")
                continue

            payload = safe_payload(msg.value())
            filename = payload.get("filename")
            frame_id = payload.get("frame_id")

            if not filename:
                continue

            image_path = os.path.join(IMAGES_DIR, filename)
            
            if not os.path.exists(image_path):
                print(f"[Aviso] Imagem não encontrada no disco: {image_path}")
                continue

            # 1. Carregar a imagem
            image = Image.open(image_path).convert("RGB")
            
            # 2. Processar através do CLIP
            inputs = processor(images=image, return_tensors="pt").to(device)
            
            with torch.no_grad():
                image_features = model.get_image_features(**inputs)
            
            # 3. Normalizar e converter para Numpy (dimensão 512)
            image_features = image_features / image_features.norm(p=2, dim=-1, keepdim=True)
            embedding_512 = image_features.cpu().numpy()[0].tolist() # Converter para lista para serialização JSON

            # 4. Criar o payload de saída
            output_payload = {
                "frame_id": frame_id,
                "timestamp_s": payload.get("timestamp_s"),
                "imu_ref_idx": payload.get("imu_ref_idx"),
                "embedding": embedding_512
            }

            # 5. Publicar no Kafka para o Edge Server consumir
            producer.produce(
                topic=TOPIC_OUT,
                key=str(frame_id).encode('utf-8'),
                value=json.dumps(output_payload).encode('utf-8')
            )
            producer.poll(0)
            
            print(f"[CLIP Encoder] Frame {frame_id} processado. Embedding (512,) publicado em '{TOPIC_OUT}'.")

    except KeyboardInterrupt:
        print("\n[CLIP Encoder] A encerrar...")
    finally:
        consumer.close()
        producer.flush()

if __name__ == "__main__":
    main()
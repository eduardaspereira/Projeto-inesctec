import json
import numpy as np
from sklearn.decomposition import PCA
from confluent_kafka import Consumer

# 1. Configuração do Consumidor Kafka (Lê do NC_topic)
consumer_conf = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'network_controller_group',
    'auto.offset.reset': 'latest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['NC_topic'])

# NOTA: Num cenário real, o Nelson tem de carregar o EXATO MESMO modelo PCA 
# que tu treinaste no Vertical Container (normalmente guardado num ficheiro .pkl).
# Aqui vamos criar um dummy apenas para não dar erro no código.
dummy_pca = PCA(n_components=256)
dummy_pca.fit(np.random.rand(300, 768)) # Simula o fit do teu lado

def dequantize_from_int8(quantized_list, scale, min_val):
    """ Reverte a quantização de INT8 para Float32 """
    quantized_array = np.array(quantized_list, dtype=np.int8)
    # Fórmula inversa: (valor_int8 + 128) * scale + min_val
    dequantized = (quantized_array + 128) * scale + min_val
    return dequantized.astype(np.float32)

print("Network Controller do Nelson à escuta no NC_topic...")

try:
    while True:
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
        if msg.error():
            print(f"Erro no Consumer do NC: {msg.error()}")
            continue

        # 1. Receber os dados do tópico
        payload = json.loads(msg.value().decode('utf-8'))
        timestamp = payload.get("timestamp")
        
        print(f"\n[{timestamp}s] Alerta recebido do Vertical Container!")
        
        # 2. Extrair dados comprimidos e parâmetros de quantização
        quantized_vector = payload.get("obstacle_state_vector")
        scale = payload["quantization_params"]["scale"]
        min_val = payload["quantization_params"]["min_val"]
        
        # 3. Descompressão Parte 1: Dequantização (INT8 -> Float32)
        reduced_vector = dequantize_from_int8(quantized_vector, scale, min_val)
        
        # 4. Descompressão Parte 2: Transformação Inversa do PCA (256 -> 768 dimensões)
        # z^-1 (lambda_t)
        original_embedding = dummy_pca.inverse_transform([reduced_vector])[0]
        
        print(f" 🔓 Descompressão concluída. Embedding reconstruído com shape: {original_embedding.shape}")
        
        # 5. Lógica de Decisão do Nelson (Roaming, Ajuste de AP, etc.)
        print(" 📡 A analisar estado da rede e a ajustar parâmetros do Access Point (AP)...")
        # TODO: O Nelson insere aqui a lógica de controlo da rede

except KeyboardInterrupt:
    print("A encerrar o Network Controller...")
finally:
    consumer.close()
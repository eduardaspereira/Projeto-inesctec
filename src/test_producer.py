import csv
import json
import time
from confluent_kafka import Producer

# Configuração do Produtor Kafka
conf = {
    'bootstrap.servers': 'localhost:29092',
    'client.id': 'test_synthetic_producer'
}
producer = Producer(conf)

def delivery_report(err, msg):
    """ Callback executado quando a mensagem é entregue ou falha. """
    if err is not None:
        print(f"Erro ao entregar mensagem no tópico {msg.topic()}: {err}")
    # else:
    #     print(f"Mensagem entregue no {msg.topic()} [Partição: {msg.partition()}]")

def load_csv(filepath):
    """ Lê um ficheiro CSV e retorna uma lista de dicionários. """
    data = []
    try:
        with open(filepath, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(row)
    except FileNotFoundError:
        print(f"Aviso: Ficheiro não encontrado - {filepath}")
    return data

def load_json(filepath):
    """ Lê um ficheiro JSON. """
    try:
        with open(filepath, mode='r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Aviso: Ficheiro não encontrado - {filepath}")
        return {"frames": []}

def main():
    print("A carregar os datasets sintéticos...")
    
    # Caminhos dos ficheiros (ajusta se estiverem noutra pasta)
    imu_data = load_csv('../datasets/processed/uav_data/imu_data.csv')
    tos_events = load_csv('../datasets/processed/tos_data/synthetic_port_events.csv')
    ran_kpms = load_csv('../datasets/processed/uav_data/ran_kpms.csv')
    images_metadata = load_json('../datasets/processed/uav_data/images_metadata.json')
    
    # Criar dicionários para acesso rápido usando o timestamp como chave
    # Arredondamos para 2 casas decimais para facilitar o matching
    images_by_time = {round(frame["timestamp_s"], 2): frame for frame in images_metadata.get("frames", [])}
    ran_by_time = {round(float(row["timestamp_s"]), 2): row for row in ran_kpms if row.get("timestamp_s")}
    
    # Como os TOS events têm timestamps absolutos, vamos usar um índice sequencial para os injetar aos poucos
    tos_index = 0
    
    print("A iniciar o streaming de dados para o Kafka...")
    
    # Usamos o IMU como relógio principal (já que é o que tem maior frequência)
    for row in imu_data:
        # Ignorar linhas vazias que possam existir no final do CSV
        if not row.get("timestamp_s") or row["timestamp_s"].strip() == "":
            continue
        current_time_s = round(float(row["timestamp_s"]), 2)
        
        # 1. Preparar Payload do Vertical Container (VC_topic)
        vc_payload = {
            "timestamp": current_time_s,
            "imu_data": row,
            "image_frame": None,
            "tos_data": None
        }
        
        # Verificar se há imagem para este instante
        if current_time_s in images_by_time:
            vc_payload["image_frame"] = f"../datasets/raw/uav_images/{images_by_time[current_time_s]['filename']}"
            
        # Injetar evento TOS sintético de vez em quando (ex: a cada 0.5s para simular eventos)
        if current_time_s % 0.5 == 0 and tos_index < len(tos_events):
            vc_payload["tos_data"] = tos_events[tos_index]
            tos_index += 1
            
        # Publicar no VC_topic
        producer.produce(
            'VC_topic',
            key=str(current_time_s),
            value=json.dumps(vc_payload),
            callback=delivery_report
        )
        
        # 2. Preparar Payload do Network Controller (NC_topic_raw)
        # Se existirem métricas RAN para este momento, publica num tópico para o NC
        if current_time_s in ran_by_time:
            producer.produce(
                'NC_topic', # Ou 'RAN_topic' consoante a tua arquitetura com o Nelson
                key=str(current_time_s),
                value=json.dumps(ran_by_time[current_time_s]),
                callback=delivery_report
            )
            
        producer.poll(0) # Processa callbacks pendentes
        
        # Simular o tempo real (dorme a diferença de tempo entre frames, aprox 0.01s para 100Hz)
        time.sleep(0.01)

    producer.flush()
    print("Streaming concluído.")

if __name__ == "__main__":
    main()
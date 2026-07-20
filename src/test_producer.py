import csv
import json
import time
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'localhost:29092',
    'client.id': 'real_data_producer'
}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"[Erro] Falha de transmissao Kafka para o topico {msg.topic()}: {err}")

def load_csv(filepath):
    data = []
    try:
        with open(filepath, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(row)
    except FileNotFoundError:
        print(f"[Aviso] Ficheiro de telemetria ausente: {filepath}")
    return data

def load_json(filepath):
    try:
        with open(filepath, mode='r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"[Aviso] Ficheiro de metadados ausente: {filepath}")
        return {"frames": []}

def main():
    print("[Inicializacao] A carregar datasets reais do terreno...")
    
    imu_data = load_csv('../datasets/processed/uav_data/imu_data.csv')
    tos_events = load_csv('../datasets/processed/tos_data/synthetic_port_events.csv')
    ran_kpms = load_csv('../datasets/processed/uav_data/ran_kpms.csv')
    images_metadata = load_json('../datasets/processed/uav_data/images_metadata.json')
    
    images_by_time = {round(frame["timestamp_s"], 2): frame for frame in images_metadata.get("frames", [])}
    ran_by_time = {round(float(row["timestamp_s"]), 2): row for row in ran_kpms if row.get("timestamp_s")}
    
    tos_index = 0
    
    print("[Sistema] A iniciar o streaming rigoroso de dados para o Kafka...")
    
    for row in imu_data:
        if not row.get("timestamp_s") or row["timestamp_s"].strip() == "":
            continue
            
        current_time_s = round(float(row["timestamp_s"]), 2)
        
        vc_payload = {
            "timestamp": current_time_s,
            "imu_data": row,
            "image_frame": None,
            "tos_data": None
        }
        
        if current_time_s in images_by_time:
            vc_payload["image_frame"] = f"../datasets/raw/uav_images/{images_by_time[current_time_s]['filename']}"
            
        if current_time_s % 0.5 == 0 and tos_index < len(tos_events):
            vc_payload["tos_data"] = tos_events[tos_index]
            tos_index += 1
            
        producer.produce(
            'VC_topic',
            key=str(current_time_s),
            value=json.dumps(vc_payload),
            callback=delivery_report
        )
        
        # Isolamento: As metricas RAN sao publicadas para consumo exclusivo do NC noutro contexto
        if current_time_s in ran_by_time:
            producer.produce(
                'RAN_topic', 
                key=str(current_time_s),
                value=json.dumps(ran_by_time[current_time_s]),
                callback=delivery_report
            )
            
        producer.poll(0) 
        
        # Preservacao da cadencia cronologica da amostra (aprox. 100Hz)
        time.sleep(0.01)

    producer.flush()
    print("[Sistema] Emissao de dados finalizada.")

if __name__ == "__main__":
    main()
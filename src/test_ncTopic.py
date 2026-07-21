import csv
import json
import time
from confluent_kafka import Producer

# =========================================================
# 1. Configuração do Kafka Producer
# =========================================================
conf = {
    'bootstrap.servers': 'localhost:29092',
    'client.id': 'ran_metrics_producer'
}
producer = Producer(conf)

def delivery_report(err, msg):
    """Callback executado após cada tentativa de envio para o Kafka."""
    if err is not None:
        print(f"[Erro] Falha de transmissao Kafka para o topico {msg.topic()}: {err}")

def load_csv(filepath):
    """Carrega o ficheiro CSV e devolve uma lista de dicionários."""
    data = []
    try:
        with open(filepath, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(row)
    except FileNotFoundError:
        print(f"[Erro Crítico] Ficheiro de telemetria ausente: {filepath}")
        exit(1)
    return data

# =========================================================
# 2. Ciclo Principal de Emissão
# =========================================================
def main():
    ran_filepath = '../datasets/processed/uav_data/ran_kpms_noise_ext.csv'
    print(f"[Inicializacao] A carregar exclusivamente métricas RAN de: {ran_filepath}")
    
    ran_kpms = load_csv(ran_filepath)
    
    print(f"[Sistema] Encontrados {len(ran_kpms)} registos de rede.")
    print("[Sistema] A iniciar o streaming de KPIs para o 'NC_topic'...\n")
    
    mensagens_enviadas = 0

    for row in ran_kpms:
        # Validar se a linha tem timestamp válido
        if not row.get("timestamp_s") or str(row["timestamp_s"]).strip() == "":
            continue
            
        current_time_s = round(float(row["timestamp_s"]), 2)
        
        # Estruturar o objeto apenas com os dados RAN, garantindo que o NC o reconheça.
        # Envolvemos as métricas na chave "ran_metrics" para manter a coerência com
        # o que o network_controller.py espera ler (payload.get("ran_metrics")).
        nc_payload = {
            "timestamp": current_time_s,
            "ran_metrics": {
                "timestamp_s": row.get("timestamp_s"),
                "report_idx": row.get("report_idx"),
                "rsrp_dbm": row.get("rsrp_dbm"),
                "rsrq_db": row.get("rsrq_db"),
                "sinr_db": row.get("sinr_db"),
                "cqi": row.get("cqi"),
                "mcs": row.get("mcs"),
                "prb_allocated": row.get("prb_allocated"),
                "prb_utilisation_pct": row.get("prb_utilisation_pct"),
                "dl_throughput_mbps": row.get("dl_throughput_mbps"),
                "ul_throughput_mbps": row.get("ul_throughput_mbps"),
                "bler": row.get("bler"),
                "latency_ms": row.get("latency_ms")
            }
        }
        
        # Emitir diretamente para o Network Controller Topic
        producer.produce(
            'NC_topic',
            key=str(current_time_s),
            value=json.dumps(nc_payload),
            callback=delivery_report
        )
        
        # Poll para libertar callbacks pendentes
        producer.poll(0)
        mensagens_enviadas += 1
        
        # Simular a cadência de rede
        time.sleep(1)

    # Garantir que todas as mensagens na fila são enviadas antes de fechar
    producer.flush()
    print(f"\n[Sistema] Emissao finalizada. {mensagens_enviadas} pacotes de rede enviados com sucesso.")

if __name__ == "__main__":
    main()
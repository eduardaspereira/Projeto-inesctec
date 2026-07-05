import json
import pandas as pd
import os

# Caminhos locais para os teus dados de entrada
PATH_IMU = r"datasets\raw\uav_data\imu_data.json"
PATH_METADATA = r"datasets\raw\uav_data\images_metadata.json"
PATH_KPMS = r"datasets\raw\uav_data\ran_kpms.json"
PATH_TOS = r"datasets\raw\tos_data\synthetic_port_events.csv"

# Caminho para a pasta de saída
OUTPUT_DIR = r"datasets\processed"

def process_and_clean_pipeline():
    print("=== A iniciar a limpeza e estruturação da pipeline ===")
    
    # Garantir que a pasta de destino existe
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"-> Pasta criada com sucesso: {OUTPUT_DIR}")
    
    # 1. Carregar IMU (A nossa fonte primária de estado físico)
    with open(PATH_IMU, 'r', encoding='utf-8') as f:
        imu_raw = json.load(f)
    df_imu = pd.DataFrame(imu_raw['samples'])
    print(f"-> IMU carregado: {len(df_imu)} amostras físicas.")

    # 2. Carregar e Filtrar Metadados da Câmara (Manter apenas o essencial para o CLIP)
    with open(PATH_METADATA, 'r', encoding='utf-8') as f:
        meta_raw = json.load(f)
    df_meta = pd.DataFrame(meta_raw['frames'])
    
    # Extrair apenas o necessário para o pipeline visual
    df_meta_clean = df_meta[['frame_id', 'timestamp_s', 'filename', 'imu_ref_idx']].copy()
    print(f"-> Metadados da Câmara limpos (Redundâncias de altitude/fase removidas).")

    # 3. Carregar e Filtrar Métricas de Rede (RAN KPMs)
    with open(PATH_KPMS, 'r', encoding='utf-8') as f:
        ran_raw = json.load(f)
    
    # Como os KPMs estão estruturados por UE ("UE-01"), vamos planificar o JSON
    ran_frames = []
    for ue_id, reports in ran_raw.items():
        df_ue = pd.DataFrame(reports)
        df_ue['ue_id'] = ue_id
        ran_frames.append(df_ue)
    df_ran = pd.concat(ran_frames, ignore_index=True)
    
    # Prunar variáveis desnecessárias da rede
    ran_columns_to_keep = [
        'timestamp_s', 'ue_id', 'report_idx', 'dist_3d_m', 'elevation_angle_deg',
        'rsrp_dbm', 'rsrq_db', 'sinr_db', 'cqi', 'mcs', 'prb_allocated', 
        'prb_utilisation_pct', 'dl_throughput_mbps', 'ul_throughput_mbps', 'bler', 'latency_ms'
    ]
    df_ran_clean = df_ran[ran_columns_to_keep].copy()
    print(f"-> Métricas O-RAN limpas (Removidas coordenadas X/Y e duplicações de altitude).")

    # 4. Carregar e Filtrar Dados Logísticos (TOS)
    df_tos = pd.read_csv(PATH_TOS)
    # Remover o event_id (UUID longo) para poupar largura de banda após a ingestão
    tos_columns_to_keep = ['event_type', 'entity_id', 'entity_type', 'terminal', 'timestamp']
    df_tos_clean = df_tos[tos_columns_to_keep].copy()
    print(f"-> Dados do TOS limpos (event_id descartado).")

    # 5. Guardar os dados limpos na pasta datasets\processed
    df_imu.to_csv(os.path.join(OUTPUT_DIR, "imu_data_clean.csv"), index=False)
    
    # Para os metadados da câmara, mantemos o formato estruturado JSON que tinhas originalmente
    meta_clean_dict = {"frames": df_meta_clean.to_dict(orient="records")}
    with open(os.path.join(OUTPUT_DIR, "images_metadata_clean.json"), "w", encoding="utf-8") as f:
        json.dump(meta_clean_dict, f, indent=4, ensure_ascii=False)
        
    df_ran_clean.to_csv(os.path.join(OUTPUT_DIR, "ran_kpms_clean.csv"), index=False)
    df_tos_clean.to_csv(os.path.join(OUTPUT_DIR, "synthetic_port_events_clean.csv"), index=False)
    print(f"-> Todos os ficheiros limpos foram gravados em: {OUTPUT_DIR}")

    print("\n=== Estrutura de Amostragem Pronta para Transmissão ===")
    print(f"Formato do Payload da Câmara para CLIP: {df_meta_clean.iloc[0].to_dict()}")
    print(f"Formato das Métricas de Rede prontas para Kafka: {df_ran_clean.iloc[0].to_dict()}")
    
    return df_imu, df_meta_clean, df_ran_clean, df_tos_clean

if __name__ == "__main__":
    # Garante que os caminhos existem antes de correr
    try:
        df_imu, df_camera, df_ran, df_tos = process_and_clean_pipeline()
        print("\n[Sucesso] Os dados foram tratados e as redundâncias eliminadas com sucesso!")
    except FileNotFoundError as e:
        print(f"\n[Erro] Verifica os caminhos dos ficheiros no teu PC: {e}")
import json
import pandas as pd
import os

# Caminhos locais para os teus dados de entrada (ajustados para Linux/WSL2)
PATH_IMU = "../datasets/raw/uav_data/imu_data.json"
PATH_METADATA = "../datasets/raw/uav_data/images_metadata.json"
PATH_KPMS = "../datasets/raw/uav_data/ran_kpms.json"
PATH_KPMS_NOISE = "../datasets/raw/uav_data/ran_kpms_noise_ext.json"  # Ficheiro de ruído adicionado
PATH_TOS = "../datasets/raw/tos_data/synthetic_port_events.csv"

# Caminho base para a pasta de saída e respetivas subpastas
OUTPUT_DIR = "../datasets/processed"
OUTPUT_UAV_DIR = os.path.join(OUTPUT_DIR, "uav_data")
OUTPUT_TOS_DIR = os.path.join(OUTPUT_DIR, "tos_data")

def helper_flatten_ran_json(ran_raw):
    """Função auxiliar para planificar o JSON da RAN estruturado por utilizador (UE)."""
    ran_frames = []
    for ue_id, reports in ran_raw.items():
        df_ue = pd.DataFrame(reports)
        df_ue['ue_id'] = ue_id
        ran_frames.append(df_ue)
    df_ran = pd.concat(ran_frames, ignore_index=True)
    
    # Colunas críticas a manter (filtrando redundâncias de altitude e coordenadas X/Y)
    columns_to_keep = [
        'timestamp_s', 'ue_id', 'report_idx', 'dist_3d_m', 'elevation_angle_deg',
        'rsrp_dbm', 'rsrq_db', 'sinr_db', 'cqi', 'mcs', 'prb_allocated', 
        'prb_utilisation_pct', 'dl_throughput_mbps', 'ul_throughput_mbps', 'bler', 'latency_ms'
    ]
    return df_ran[columns_to_keep].copy()

def process_and_clean_pipeline():
    print("=== A iniciar a limpeza e estruturação da pipeline ===")
    
    # Garantir que as subpastas de destino existem no Linux
    os.makedirs(OUTPUT_UAV_DIR, exist_ok=True)
    os.makedirs(OUTPUT_TOS_DIR, exist_ok=True)
    print(f"-> Estrutura de pastas verificada/criada em: {OUTPUT_DIR}")
    
    # 1. Carregar IMU
    with open(PATH_IMU, 'r', encoding='utf-8') as f:
        imu_raw = json.load(f)
    df_imu = pd.DataFrame(imu_raw['samples'])
    print(f"-> IMU carregado: {len(df_imu)} amostras físicas.")

    # 2. Carregar e Filtrar Metadados da Câmara (Essencial para o CLIP)
    with open(PATH_METADATA, 'r', encoding='utf-8') as f:
        meta_raw = json.load(f)
    df_meta = pd.DataFrame(meta_raw['frames'])
    df_meta_clean = df_meta[['frame_id', 'timestamp_s', 'filename', 'imu_ref_idx']].copy()
    print("-> Metadados da Câmara limpos (Redundâncias físicas removidas).")

    # 3. Carregar e Filtrar Métricas de Rede Limpas (RAN KPMs)
    with open(PATH_KPMS, 'r', encoding='utf-8') as f:
        ran_raw = json.load(f)
    df_ran_clean = helper_flatten_ran_json(ran_raw)
    print("-> Métricas O-RAN Padrão limpas com sucesso.")

    # 4. Carregar e Filtrar Métricas de Rede Realistas (RAN KPMs com Ruído)
    with open(PATH_KPMS_NOISE, 'r', encoding='utf-8') as f:
        ran_noise_raw = json.load(f)
    df_ran_noise_clean = helper_flatten_ran_json(ran_noise_raw)
    print("-> Métricas O-RAN com Ruído Extendido limpas com sucesso (Casas decimais preservadas).")

    # 5. Carregar e Filtrar Dados Logísticos (TOS)
    df_tos = pd.read_csv(PATH_TOS)
    tos_columns_to_keep = ['event_type', 'entity_id', 'entity_type', 'terminal', 'timestamp']
    df_tos_clean = df_tos[tos_columns_to_keep].copy()
    print("-> Dados do TOS limpos (event_id UUID removido).")

    # 6. Guardar os dados limpos nas subpastas da pipeline
    df_imu.to_csv(os.path.join(OUTPUT_UAV_DIR, "imu_data.csv"), index=False)
    df_ran_clean.to_csv(os.path.join(OUTPUT_UAV_DIR, "ran_kpms.csv"), index=False)
    df_ran_noise_clean.to_csv(os.path.join(OUTPUT_UAV_DIR, "ran_kpms_noise_ext.csv"), index=False)
    df_tos_clean.to_csv(os.path.join(OUTPUT_TOS_DIR, "synthetic_port_events.csv"), index=False)
    
    # Salvar metadados da câmara em formato JSON estruturado limpo
    meta_clean_dict = {"frames": df_meta_clean.to_dict(orient="records")}
    with open(os.path.join(OUTPUT_UAV_DIR, "images_metadata.json"), "w", encoding="utf-8") as f:
        json.dump(meta_clean_dict, f, indent=4, ensure_ascii=False)
        
    print("-> Todos os ficheiros limpos foram organizados com sucesso.")
    return df_imu, df_meta_clean, df_ran_clean, df_ran_noise_clean, df_tos_clean

if __name__ == "__main__":
    try:
        res = process_and_clean_pipeline()
        print("\n[Sucesso] Execução completa! O teu ambiente local de dados está pronto.")
    except FileNotFoundError as e:
        print(f"\n[Erro] Verifica se a tua árvore de diretórios corresponde ao caminho: {e}")
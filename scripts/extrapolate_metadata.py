import json
import copy

base_data = {
    "frames": [
        {"frame_id": 0, "timestamp_s": 0.0, "filename": "frame_0000.png", "shape": [224, 224, 3], "imu_ref_idx": 0, "altitude_m": 0.15, "speed_mps": 0.0, "heading_deg": 45.0, "roll_deg": 0.0, "pitch_deg": 8.0, "flight_phase": "takeoff", "cam_pan_x": 112, "cam_pan_y": 112},
        {"frame_id": 1, "timestamp_s": 0.333, "filename": "frame_0001.png", "shape": [224, 224, 3], "imu_ref_idx": 33, "altitude_m": 10.27, "speed_mps": 1.239, "heading_deg": 45.0, "roll_deg": 0.0, "pitch_deg": 8.0, "flight_phase": "takeoff", "cam_pan_x": 112, "cam_pan_y": 112},
        {"frame_id": 2, "timestamp_s": 0.667, "filename": "frame_0002.png", "shape": [224, 224, 3], "imu_ref_idx": 66, "altitude_m": 20.06, "speed_mps": 2.477, "heading_deg": 45.0, "roll_deg": 0.0, "pitch_deg": 8.0, "flight_phase": "takeoff", "cam_pan_x": 112, "cam_pan_y": 112},
        {"frame_id": 3, "timestamp_s": 1.0, "filename": "frame_0003.png", "shape": [224, 224, 3], "imu_ref_idx": 100, "altitude_m": 30.09, "speed_mps": 3.754, "heading_deg": 45.0, "roll_deg": 0.0, "pitch_deg": 8.0, "flight_phase": "takeoff", "cam_pan_x": 112, "cam_pan_y": 112},
        {"frame_id": 4, "timestamp_s": 1.333, "filename": "frame_0004.png", "shape": [224, 224, 3], "imu_ref_idx": 133, "altitude_m": 40.49, "speed_mps": 4.992, "heading_deg": 45.0, "roll_deg": 0.0, "pitch_deg": 8.0, "flight_phase": "takeoff", "cam_pan_x": 112, "cam_pan_y": 112},
        {"frame_id": 5, "timestamp_s": 1.667, "filename": "frame_0005.png", "shape": [224, 224, 3], "imu_ref_idx": 166, "altitude_m": 50.13, "speed_mps": 6.231, "heading_deg": 45.0, "roll_deg": 0.0, "pitch_deg": 8.0, "flight_phase": "takeoff", "cam_pan_x": 113, "cam_pan_y": 113},
        {"frame_id": 6, "timestamp_s": 2.0, "filename": "frame_0006.png", "shape": [224, 224, 3], "imu_ref_idx": 200, "altitude_m": 60.31, "speed_mps": 15.0, "heading_deg": 45.0, "roll_deg": 0.0, "pitch_deg": 2.0, "flight_phase": "cruise", "cam_pan_x": 115, "cam_pan_y": 115},
        {"frame_id": 7, "timestamp_s": 2.333, "filename": "frame_0007.png", "shape": [224, 224, 3], "imu_ref_idx": 233, "altitude_m": 60.48, "speed_mps": 15.0, "heading_deg": 47.5, "roll_deg": 0.0, "pitch_deg": 2.0, "flight_phase": "cruise", "cam_pan_x": 117, "cam_pan_y": 117},
        {"frame_id": 8, "timestamp_s": 2.667, "filename": "frame_0008.png", "shape": [224, 224, 3], "imu_ref_idx": 266, "altitude_m": 60.26, "speed_mps": 15.0, "heading_deg": 50.0, "roll_deg": 0.0, "pitch_deg": 2.0, "flight_phase": "cruise", "cam_pan_x": 120, "cam_pan_y": 119},
        {"frame_id": 9, "timestamp_s": 3.0, "filename": "frame_0009.png", "shape": [224, 224, 3], "imu_ref_idx": 300, "altitude_m": 60.37, "speed_mps": 15.0, "heading_deg": 52.5, "roll_deg": 0.0, "pitch_deg": 2.0, "flight_phase": "cruise", "cam_pan_x": 123, "cam_pan_y": 121},
        {"frame_id": 10, "timestamp_s": 3.333, "filename": "frame_0010.png", "shape": [224, 224, 3], "imu_ref_idx": 333, "altitude_m": 60.31, "speed_mps": 15.0, "heading_deg": 55.0, "roll_deg": 0.0, "pitch_deg": 2.0, "flight_phase": "cruise", "cam_pan_x": 126, "cam_pan_y": 123},
        {"frame_id": 11, "timestamp_s": 3.667, "filename": "frame_0011.png", "shape": [224, 224, 3], "imu_ref_idx": 366, "altitude_m": 60.18, "speed_mps": 15.0, "heading_deg": 57.5, "roll_deg": 0.0, "pitch_deg": 2.0, "flight_phase": "cruise", "cam_pan_x": 129, "cam_pan_y": 125},
        {"frame_id": 12, "timestamp_s": 4.0, "filename": "frame_0012.png", "shape": [224, 224, 3], "imu_ref_idx": 400, "altitude_m": 60.52, "speed_mps": 15.0, "heading_deg": 60.0, "roll_deg": 12.0, "pitch_deg": 2.0, "flight_phase": "cruise", "cam_pan_x": 132, "cam_pan_y": 126},
        {"frame_id": 13, "timestamp_s": 4.333, "filename": "frame_0013.png", "shape": [224, 224, 3], "imu_ref_idx": 433, "altitude_m": 60.19, "speed_mps": 15.0, "heading_deg": 62.5, "roll_deg": 12.0, "pitch_deg": 2.0, "flight_phase": "cruise", "cam_pan_x": 135, "cam_pan_y": 127},
        {"frame_id": 14, "timestamp_s": 4.667, "filename": "frame_0014.png", "shape": [224, 224, 3], "imu_ref_idx": 466, "altitude_m": 60.18, "speed_mps": 15.0, "heading_deg": 65.0, "roll_deg": 12.0, "pitch_deg": 2.0, "flight_phase": "cruise", "cam_pan_x": 138, "cam_pan_y": 128},
        {"frame_id": 15, "timestamp_s": 5.0, "filename": "frame_0015.png", "shape": [224, 224, 3], "imu_ref_idx": 500, "altitude_m": 60.23, "speed_mps": 15.0, "heading_deg": 67.5, "roll_deg": 12.0, "pitch_deg": 2.0, "flight_phase": "cruise", "cam_pan_x": 141, "cam_pan_y": 129},
        {"frame_id": 16, "timestamp_s": 5.333, "filename": "frame_0016.png", "shape": [224, 224, 3], "imu_ref_idx": 533, "altitude_m": 60.15, "speed_mps": 15.0, "heading_deg": 70.0, "roll_deg": 12.0, "pitch_deg": 2.0, "flight_phase": "cruise", "cam_pan_x": 144, "cam_pan_y": 130},
        {"frame_id": 17, "timestamp_s": 5.667, "filename": "frame_0017.png", "shape": [224, 224, 3], "imu_ref_idx": 566, "altitude_m": 60.31, "speed_mps": 15.0, "heading_deg": 72.5, "roll_deg": 12.0, "pitch_deg": 2.0, "flight_phase": "cruise", "cam_pan_x": 147, "cam_pan_y": 131},
        {"frame_id": 18, "timestamp_s": 6.0, "filename": "frame_0018.png", "shape": [224, 224, 3], "imu_ref_idx": 600, "altitude_m": 60.08, "speed_mps": 2.0, "heading_deg": 75.1, "roll_deg": 0.0, "pitch_deg": 2.0, "flight_phase": "loiter", "cam_pan_x": 147, "cam_pan_y": 131},
        {"frame_id": 19, "timestamp_s": 6.333, "filename": "frame_0019.png", "shape": [224, 224, 3], "imu_ref_idx": 633, "altitude_m": 60.34, "speed_mps": 2.0, "heading_deg": 82.6, "roll_deg": 0.0, "pitch_deg": 2.0, "flight_phase": "loiter", "cam_pan_x": 147, "cam_pan_y": 131},
        {"frame_id": 20, "timestamp_s": 6.667, "filename": "frame_0020.png", "shape": [224, 224, 3], "imu_ref_idx": 666, "altitude_m": 60.22, "speed_mps": 2.0, "heading_deg": 90.0, "roll_deg": 0.0, "pitch_deg": 2.0, "flight_phase": "loiter", "cam_pan_x": 147, "cam_pan_y": 131},
        {"frame_id": 21, "timestamp_s": 7.0, "filename": "frame_0021.png", "shape": [224, 224, 3], "imu_ref_idx": 700, "altitude_m": 60.4, "speed_mps": 2.0, "heading_deg": 97.7, "roll_deg": 0.0, "pitch_deg": 2.0, "flight_phase": "loiter", "cam_pan_x": 147, "cam_pan_y": 130},
        {"frame_id": 22, "timestamp_s": 7.333, "filename": "frame_0022.png", "shape": [224, 224, 3], "imu_ref_idx": 733, "altitude_m": 60.12, "speed_mps": 2.0, "heading_deg": 105.1, "roll_deg": 0.0, "pitch_deg": 2.0, "flight_phase": "loiter", "cam_pan_x": 147, "cam_pan_y": 129},
        {"frame_id": 23, "timestamp_s": 7.667, "filename": "frame_0023.png", "shape": [224, 224, 3], "imu_ref_idx": 766, "altitude_m": 60.24, "speed_mps": 2.0, "heading_deg": 112.5, "roll_deg": 0.0, "pitch_deg": 2.0, "flight_phase": "loiter", "cam_pan_x": 147, "cam_pan_y": 128},
        {"frame_id": 24, "timestamp_s": 8.0, "filename": "frame_0024.png", "shape": [224, 224, 3], "imu_ref_idx": 800, "altitude_m": 59.96, "speed_mps": 14.94, "heading_deg": 120.0, "roll_deg": 0.0, "pitch_deg": -5.0, "flight_phase": "descent", "cam_pan_x": 150, "cam_pan_y": 126},
        {"frame_id": 25, "timestamp_s": 8.333, "filename": "frame_0025.png", "shape": [224, 224, 3], "imu_ref_idx": 833, "altitude_m": 50.19, "speed_mps": 12.462, "heading_deg": 120.0, "roll_deg": 0.0, "pitch_deg": -5.0, "flight_phase": "descent", "cam_pan_x": 152, "cam_pan_y": 124},
        {"frame_id": 26, "timestamp_s": 8.667, "filename": "frame_0026.png", "shape": [224, 224, 3], "imu_ref_idx": 866, "altitude_m": 39.98, "speed_mps": 9.985, "heading_deg": 120.0, "roll_deg": 0.0, "pitch_deg": -5.0, "flight_phase": "descent", "cam_pan_x": 154, "cam_pan_y": 122},
        {"frame_id": 27, "timestamp_s": 9.0, "filename": "frame_0027.png", "shape": [224, 224, 3], "imu_ref_idx": 900, "altitude_m": 30.09, "speed_mps": 7.432, "heading_deg": 120.0, "roll_deg": 0.0, "pitch_deg": -5.0, "flight_phase": "descent", "cam_pan_x": 155, "cam_pan_y": 121},
        {"frame_id": 28, "timestamp_s": 9.333, "filename": "frame_0028.png", "shape": [224, 224, 3], "imu_ref_idx": 933, "altitude_m": 19.82, "speed_mps": 4.955, "heading_deg": 120.0, "roll_deg": 0.0, "pitch_deg": -5.0, "flight_phase": "descent", "cam_pan_x": 156, "cam_pan_y": 120},
        {"frame_id": 29, "timestamp_s": 9.667, "filename": "frame_0029.png", "shape": [224, 224, 3], "imu_ref_idx": 966, "altitude_m": 10.3, "speed_mps": 2.477, "heading_deg": 120.0, "roll_deg": 0.0, "pitch_deg": -5.0, "flight_phase": "descent", "cam_pan_x": 156, "cam_pan_y": 119},
        {"frame_id": 30, "timestamp_s": 10.0, "filename": "frame_0030.png", "shape": [224, 224, 3], "imu_ref_idx": 999, "altitude_m": 0.52, "speed_mps": 0.0, "heading_deg": 120.0, "roll_deg": 0.0, "pitch_deg": -5.0, "flight_phase": "descent", "cam_pan_x": 156, "cam_pan_y": 119}
    ],
    "metadata": {
        "fps": 3,
        "n_frames": 31,
        "resolution": [224, 224, 3],
        "format": "PNG RGB",
        "noise_model": "Poissonian-Gaussian (Foi et al. 2008)",
        "effects": ["motion_blur", "vignetting", "sensor_noise", "altitude_zoom", "shadow_shift", "roll_tilt"]
    }
}

source_frames = base_data["frames"]
cycle_len = len(source_frames)  # 31 frames (0 a 30)
target_frames_count = 318  # De 0 a 317

full_frames = []

for i in range(target_frames_count):
    # Encontra o índice correspondente dentro do ciclo base de 31 frames
    base_idx = i % cycle_len
    # Determina em que ciclo de simulação de voo estamos (0, 1, 2, ...)
    cycle_num = i // cycle_len
    
    # Faz uma cópia profunda da frame modelo para não corromper os dados originais
    new_frame = copy.deepcopy(source_frames[base_idx])
    
    # Atualiza as propriedades dinâmicas e de índice temporal
    new_frame["frame_id"] = i
    new_frame["filename"] = f"frame_{i:04d}.png"
    
    # O timestamp avança linearmente (3 frames por segundo -> delta_t = 0.33333s)
    new_frame["timestamp_s"] = round(i * (1.0 / 3.0), 3)
    
    # O índice do IMU avança linearmente a cada frame
    new_frame["imu_ref_idx"] = i * 33
    
    full_frames.append(new_frame)

# Atualiza a estrutura final
output_data = copy.deepcopy(base_data)
output_data["frames"] = full_frames
output_data["metadata"]["n_frames"] = target_frames_count

output_filename = "images_metadata_full.json"
with open(output_filename, "w", encoding="utf-8") as f:
    json.dump(output_data, f, indent=2)

print(f"Sucesso! {target_frames_count} frames guardados em '{output_filename}'.")
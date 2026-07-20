import cv2
import numpy as np
import os
import json

def gerar_fundo_texturizado(largura=500, altura=500):
    cor_base = (80, 115, 95) 
    img = np.zeros((altura, largura, 3), dtype=np.uint8)
    img[:] = cor_base
    ruido = np.random.randint(-25, 25, (altura, largura, 3), dtype=np.int16)
    img = np.clip(img.astype(np.int16) + ruido, 0, 255).astype(np.uint8)
    img = cv2.GaussianBlur(img, (3, 3), 0)
    return img

def gerar_dataset_318(num_frames=318, num_ues=3, pasta_saida="../datasets/raw/uav_images/dataset_sintetico"):
    os.makedirs(pasta_saida, exist_ok=True)
    metadados = {"frames": []}
    
    print(f"[Gerador] A iniciar simulação contínua de exatamente {num_frames} frames com {num_ues} UEs.")
    
    ap_pos = (50, 250)
    ues_pos = []
    
    # Gerar UEs espalhados no lado direito
    for _ in range(num_ues):
        ues_pos.append((np.random.randint(350, 480), np.random.randint(100, 400)))

    # Algoritmo de ressalto (Bounce 2D) para manter a trajetória fluída indefinidamente
    ct_x, ct_y = 200.0, 100.0
    vx, vy = 4.5, 3.2  # Velocidade em pixéis por frame
    
    fps = 10
    larg_ct, alt_ct = 50, 40

    for step in range(num_frames):
        img = gerar_fundo_texturizado(500, 500)
        
        # Movimento vetorial contínuo
        ct_x += vx
        ct_y += vy
        
        # Bater nas paredes invisíveis (garante que cruza o sinal constantemente)
        if ct_x < 120 or ct_x > 400: vx *= -1
        if ct_y < 50 or ct_y > 450: vy *= -1
        
        timestamp_atual = round(step * (1.0 / fps), 2)
        
        frame_meta = {
            "timestamp_s": timestamp_atual,
            "filename": f"frame_{step:04d}.png",
            "ap_pos": [ap_pos[0] + 20, ap_pos[1] + 20],
            "ues_pos": [],
            "container_pos": [int(ct_x) + larg_ct//2, int(ct_y) + alt_ct//2]
        }

        # Desenhar AP
        cv2.rectangle(img, ap_pos, (ap_pos[0] + 40, ap_pos[1] + 40), (30, 30, 30), -1)

        # Desenhar UEs no JSON
        for ue in ues_pos:
            cv2.circle(img, ue, 18, (30, 30, 30), -1)
            frame_meta["ues_pos"].append([ue[0], ue[1]])

        # Desenhar Contentor
        cv2.rectangle(img, (int(ct_x), int(ct_y)), (int(ct_x) + larg_ct, int(ct_y) + alt_ct), (20, 100, 210), -1)
        cv2.rectangle(img, (int(ct_x), int(ct_y)), (int(ct_x) + larg_ct, int(ct_y) + alt_ct), (20, 20, 20), 2)
        
        # Guardar Frame
        caminho_imagem = os.path.join(pasta_saida, frame_meta["filename"])
        cv2.imwrite(caminho_imagem, img)
        metadados["frames"].append(frame_meta)

    with open(os.path.join(pasta_saida, "images_metadata.json"), "w") as f:
        json.dump(metadados, f, indent=4)
        
    print(f"[Gerador] Concluído! 318 frames guardados em: ./{pasta_saida}/")

if __name__ == "__main__":
    gerar_dataset_318()
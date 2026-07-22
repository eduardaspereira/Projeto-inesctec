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

def gerar_dataset_318(num_frames=318, num_ues=3, pasta_saida="../datasets/raw/uav_images/dataset_sintetico2"):
    os.makedirs(pasta_saida, exist_ok=True)
    metadados = {"frames": []}
    
    largura_img, altura_img = 500, 500
    
    print(f"[Gerador] A iniciar simulação contínua de exatamente {num_frames} frames com {num_ues} UEs.")
    
    # 1. AP em sítio aleatório (com margem de segurança das bordas)
    ap_pos = (np.random.randint(50, largura_img - 90), np.random.randint(50, altura_img - 90))
    
    ues_pos = []
    
    # 2. Gerar UEs espalhados por TODO o mapa, evitando colisão inicial com o AP
    for _ in range(num_ues):
        while True:
            ue_x = np.random.randint(40, largura_img - 40)
            ue_y = np.random.randint(40, altura_img - 40)
            
            # Distância euclidiana para garantir que UEs não nascem colados ao AP
            dist_ap = np.sqrt((ue_x - ap_pos[0])**2 + (ue_y - ap_pos[1])**2)
            
            if dist_ap > 120:  # Só aceita a posição se estiver a mais de 120 pixéis do AP
                ues_pos.append((ue_x, ue_y))
                break

    # 3. Algoritmo de ressalto (Bounce 2D) adaptado para cobrir a imagem toda
    ct_x, ct_y = float(largura_img // 2), float(altura_img // 2)
    vx, vy = 4.5, 3.2  # Velocidade em pixéis por frame
    
    fps = 10
    larg_ct, alt_ct = 50, 40

    for step in range(num_frames):
        img = gerar_fundo_texturizado(largura_img, altura_img)
        
        # Movimento vetorial contínuo
        ct_x += vx
        ct_y += vy
        
        # Bater nas "paredes" reais do ecrã (0 a 500)
        if ct_x <= 0 or ct_x >= (largura_img - larg_ct): vx *= -1
        if ct_y <= 0 or ct_y >= (altura_img - alt_ct): vy *= -1
        
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
        
    print(f"[Gerador] Concluído! Frames guardados em: ./{pasta_saida}/")

if __name__ == "__main__":
    gerar_dataset_318()
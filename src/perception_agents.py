import cv2
import numpy as np
from collections import deque

class ImagePerceptionAgent:
    def __init__(self, history_size=10):
        # Memoria temporal para calcular velocidade
        self.container_history = deque(maxlen=history_size)
        
    def detect_shapes(self, image_path):
        """ Identifica as coordenadas e devolve a imagem para debug visual. """
        if not image_path or image_path == "None":
            return None, None, None, None

        img = cv2.imread(image_path)
        if img is None: 
            return None, None, None, None
        
        hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)

        # 1. Detetar Contentor (Laranja)
        lower_orange = np.array([10, 100, 100])
        upper_orange = np.array([25, 255, 255])
        mask_orange = cv2.inRange(hsv, lower_orange, upper_orange)
        contours_orange, _ = cv2.findContours(mask_orange, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        container_pos = None
        if contours_orange:
            # Assume o maior objeto laranja
            c = max(contours_orange, key=cv2.contourArea)
            M = cv2.moments(c)
            if M["m00"] != 0:
                container_pos = (int(M["m10"] / M["m00"]), int(M["m01"] / M["m00"]))

        # 2. Detetar AP (Quadrado Preto) e UE (Circulo Preto)
        lower_black = np.array([0, 0, 0])
        upper_black = np.array([180, 255, 50])
        mask_black = cv2.inRange(hsv, lower_black, upper_black)
        contours_black, _ = cv2.findContours(mask_black, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

        ap_pos, ue_pos = None, None
        for c in contours_black:
            if cv2.contourArea(c) < 50: continue # Ignora ruido
            
            peri = cv2.arcLength(c, True)
            approx = cv2.approxPolyDP(c, 0.04 * peri, True)
            
            M = cv2.moments(c)
            if M["m00"] != 0:
                cx = int(M["m10"] / M["m00"])
                cy = int(M["m01"] / M["m00"])
                
                # Quadrado (AP)
                if len(approx) == 4:
                    ap_pos = (cx, cy)
                # Circulo (UE)
                elif len(approx) > 4:
                    ue_pos = (cx, cy)

        return ap_pos, ue_pos, container_pos, img

    def analyze_frame(self, image_path, fps=30):
        """ Processa o frame, gera auditoria visual e preve a queda de sinal. """
        ap_pos, ue_pos, current_container, img = self.detect_shapes(image_path)
        
        # ========================================================
        # AUDITORIA VISUAL (VISUAL DEBUGGING)
        # ========================================================
        if img is not None:
            # Desenhar o AP a Azul
            if ap_pos:
                cv2.rectangle(img, (ap_pos[0]-15, ap_pos[1]-15), (ap_pos[0]+15, ap_pos[1]+15), (255, 0, 0), 2)
                cv2.putText(img, "AP", (ap_pos[0]-15, ap_pos[1]-20), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 0, 0), 2)

            # Desenhar o UE a Verde
            if ue_pos:
                cv2.circle(img, ue_pos, 15, (0, 255, 0), 2)
                cv2.putText(img, "UE", (ue_pos[0]-15, ue_pos[1]-20), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)

            # Traçar a Linha de Visada (LoS) a Branco
            if ap_pos and ue_pos:
                cv2.line(img, ap_pos, ue_pos, (255, 255, 255), 1)

            # Desenhar o Contentor a Vermelho
            if current_container:
                cv2.rectangle(img, (current_container[0]-25, current_container[1]-25), (current_container[0]+25, current_container[1]+25), (0, 0, 255), 2)
                cv2.putText(img, "Contentor", (current_container[0]-25, current_container[1]-30), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 0, 255), 2)

            # Guardar o frame anotado na pasta 'src'
            cv2.imwrite("teste_visao.jpg", img)
        # ========================================================

        if not ap_pos or not ue_pos or not current_container:
            return {"los_blocked_now": False, "time_to_block_s": -1.0}

        self.container_history.append(current_container)

        # Logica de intercecao fisica com buffer
        min_x, max_x = min(ap_pos[0], ue_pos[0]), max(ap_pos[0], ue_pos[0])
        min_y, max_y = min(ap_pos[1], ue_pos[1]), max(ap_pos[1], ue_pos[1])
        
        if min_x < current_container[0] < max_x and min_y - 20 < current_container[1] < max_y + 20:
            return {"los_blocked_now": True, "time_to_block_s": 0.0}

        if len(self.container_history) >= 5:
            past_pos = self.container_history[0]
            
            dx = current_container[0] - past_pos[0]
            dy = current_container[1] - past_pos[1]
            
            frames_passed = len(self.container_history)
            vx = dx / frames_passed
            vy = dy / frames_passed
            
            if vx != 0 or vy != 0:
                reta_y = (ap_pos[1] + ue_pos[1]) / 2 
                if vy != 0:
                    frames_to_intersect = (reta_y - current_container[1]) / vy
                    if frames_to_intersect > 0:
                        time_to_block_s = frames_to_intersect / fps
                        return {"los_blocked_now": False, "time_to_block_s": round(time_to_block_s, 2)}
        
        return {"los_blocked_now": False, "time_to_block_s": -1.0}

class NLPAgent:
    def __init__(self):
        pass
        
    def analyze_text(self, tos_data):
        if not tos_data:
            return {"imminent_threat": False, "reason": "No data"}
            
        event = str(tos_data.get('event_type', '')).upper()
        
        if "MOVE" in event or "CRANE_LIFT" in event:
            return {"imminent_threat": True, "reason": "Crane in motion"}
            
        return {"imminent_threat": False, "reason": "Static"}
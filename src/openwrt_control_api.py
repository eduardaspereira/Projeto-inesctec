import json
from flask import Flask, request, jsonify

app = Flask(__name__)

# Base de dados simulada para acompanhar o estado do Wireless AP
AP_STATE = {
    "interface": "wlan0",
    "tx_power_dbm": 20,
    "global_bandwidth_limit_mbps": 100,
    "connected_ues": {
        "UE_1": {"mac": "D8:07:B6:0E:99:83", "allocated_bandwidth_mbps": 50},
        "UE_2": {"mac": "A1:B2:C3:D4:E5:F6", "allocated_bandwidth_mbps": 50}
    }
}

@app.route('/api/status', methods=['GET'])
def get_status():
    """Retorna o estado atual das métricas e configurações do Wireless AP."""
    print("📡 [OpenWrt API]: Pedido de status recebido pelo Orquestrador.")
    return jsonify(AP_STATE), 200

@app.route('/api/set_bandwidth', methods=['POST'])
def set_bandwidth():
    """Ajusta dinamicamente a largura de banda de um dispositivo (Traffic Shaping)."""
    data = request.get_json()
    ue_id = data.get("ue_id")
    bandwidth = data.get("bandwidth_mbps")
    
    if ue_id in AP_STATE["connected_ues"]:
        AP_STATE["connected_ues"][ue_id]["allocated_bandwidth_mbps"] = bandwidth
        
        # Aqui simulamos o comando UCI real que vais correr via SSH no teu Archer C50
        print(f"\n⚡ [UCI COMMAND EXECUTED] -> uci set wireless.{AP_STATE['interface']}.max_rate={bandwidth}m && uci commit")
        print(f"⚖️ [OpenWrt AP]: Tráfego moldado. {ue_id} limitado a {bandwidth} Mbps.")
        
        return jsonify({"status": "success", "message": f"Bandwidth updated for {ue_id}"}), 200
    return jsonify({"status": "error", "message": "UE not found"}), 404

@app.route('/api/force_roaming', methods=['POST'])
def force_roaming():
    """Força a desconexão de um telemóvel para obrigá-lo a fazer roaming."""
    data = request.get_json()
    ue_id = data.get("ue_id")
    
    if ue_id in AP_STATE["connected_ues"]:
        mac = AP_STATE["connected_ues"][ue_id]["mac"]
        del AP_STATE["connected_ues"][ue_id]
        
        # Aqui simulamos o comando UBUS real do OpenWrt para escorraçar um cliente rádio
        print(f"\n💥 [UBUS COMMAND EXECUTED] -> ubus call hostapd.{AP_STATE['interface']} del_client '{{\"addr\":\"{mac}\", \"reason\":1, \"deauth\":true}}'")
        print(f"🚪 [OpenWrt AP]: {ue_id} ({mac}) foi desconectado via hardware para forçar Roaming.")
        
        return jsonify({"status": "success", "message": f"Forced roaming executed for {ue_id}"}), 200
    return jsonify({"status": "error", "message": "UE not found"}), 404

if __name__ == '__main__':
    print("🚀 [OpenWrt Control API] A correr em http://localhost:5000")
    print("🔒 Pronto para receber comandos de controlo rádio do Cérebro de IA...")
    app.run(host='0.0.0.0', port=5000)
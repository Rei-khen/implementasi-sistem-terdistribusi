# master.py
from flask import Flask, request, jsonify
import requests
import threading

# Inisialisasi aplikasi Flask
app = Flask(__name__)

# Penyimpanan data di memori (menggunakan dictionary)
master_data = {}

# Daftar alamat server slave yang terdaftar
# Kita akan menjalankan dua slave di port 5001 dan 5002
SLAVE_SERVERS = [
    "http://127.0.0.1:5001",
    "http://127.0.0.1:5002",
    "http://127.0.0.1:5003"  
]

def replicate_to_slaves(key, value):
    """Fungsi untuk mengirim data ke semua slave secara asynchronous."""
    for slave in SLAVE_SERVERS:
        try:
            # Kirim request POST ke endpoint /replicate di setiap slave
            requests.post(f"{slave}/replicate", json={"key": key, "value": value}, timeout=0.5)
            print(f"Replication sent to {slave} for key: {key}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to replicate to {slave}: {e}")

@app.route('/set', methods=['POST'])
def set_data():
    """Endpoint untuk menulis data baru."""
    data = request.get_json()
    key = data.get('key')
    value = data.get('value')
    
    if key is None or value is None:
        return jsonify({"error": "Invalid data"}), 400
    
    # 1. Simpan data di master
    master_data[key] = value
    print(f"Data set on master: {key} = {value}")
    
    # 2. Replikasi data ke semua slave di background thread (asynchronous)
    #    Ini agar master tidak perlu menunggu proses replikasi selesai
    thread = threading.Thread(target=replicate_to_slaves, args=(key, value))
    thread.start()
    
    return jsonify({"message": "Data set and replication initiated"}), 200

@app.route('/get/<key>', methods=['GET'])
def get_data(key):
    """Endpoint untuk membaca data dari master."""
    if key in master_data:
        return jsonify({key: master_data[key]}), 200
    else:
        return jsonify({"error": "Data not found"}), 404

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint untuk memeriksa status server."""
    return "Master is running", 200

if __name__ == '__main__':
    print("Master server running on port 5000")
    app.run(port=5000, debug=True, use_reloader=False)

# slave.py
from flask import Flask, request, jsonify
import sys
import requests
import time

# Inisialisasi aplikasi Flask
app = Flask(__name__)

# Penyimpanan data di memori (menggunakan dictionary)
slave_data = {}
pending_data = {} # Untuk menyimpan data sementara selama fase prepare

# Alamat Master Server
MASTER_URL = "http://127.0.0.1:5000"

# Mendapatkan port dari argumen command line, default 5001
port = int(sys.argv[1]) if len(sys.argv) > 1 else 5001

def sync_with_master():
    """Fungsi untuk melakukan sinkronisasi data dengan master saat startup."""
    while True:
        try:
            print(f"Slave (Port {port}) mencoba sinkronisasi dengan master...")
            response = requests.get(f"{MASTER_URL}/sync")
            if response.status_code == 200:
                global slave_data
                slave_data = response.json()
                print(f"Sinkronisasi berhasil. {len(slave_data)} data diterima.")
                break # Keluar dari loop jika berhasil
        except requests.exceptions.RequestException as e:
            print(f"Gagal terhubung ke master: {e}. Mencoba lagi dalam 5 detik...")
            time.sleep(5)


@app.route('/get/<key>', methods=['GET'])
def get_data(key):
    """Endpoint untuk membaca data dari slave."""
    if key in slave_data:
        return jsonify({key: slave_data[key]}), 200
    else:
        return jsonify({"error": "Data not found"}), 404

@app.route('/prepare', methods=['POST'])
def prepare():
    """Fase 1: Menerima permintaan, menyimpannya sementara, dan memberikan suara."""
    data = request.get_json()
    print(f"Slave (Port {port}) menerima permintaan PREPARE: {data}")
    # Simpan data di 'pending'
    global pending_data
    pending_data = data
    # Berikan suara setuju
    return jsonify({"vote": "COMMIT"}), 200

@app.route('/commit', methods=['POST'])
def commit():
    """Fase 2: Menerima perintah commit, memindahkan data dari pending ke data utama."""
    global pending_data, slave_data
    if not pending_data:
        return jsonify({"error": "No pending data to commit"}), 400
    
    key = pending_data.get('key')
    value = pending_data.get('value')
    slave_data[key] = value
    print(f"Slave (Port {port}) COMMIT berhasil: {key} = {value}")
    pending_data = {} # Kosongkan data pending
    return jsonify({"message": "Commit successful"}), 200

@app.route('/abort', methods=['POST'])
def abort():
    """Fase 2: Menerima perintah abort, membatalkan data pending."""
    global pending_data
    print(f"Slave (Port {port}) menerima ABORT. Transaksi dibatalkan.")
    pending_data = {} # Kosongkan data pending
    return jsonify({"message": "Abort successful"}), 200

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint untuk memeriksa status server."""
    return "Slave is running", 200

if __name__ == '__main__':
    print(f"Slave server running on port {port}")
    # Lakukan sinkronisasi sebelum server siap menerima permintaan
    sync_with_master()
    app.run(port=port, debug=True, use_reloader=False)

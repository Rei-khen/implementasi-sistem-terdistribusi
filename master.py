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

@app.route('/set', methods=['POST'])
def set_data():
    """Endpoint untuk menulis data baru."""
    data = request.get_json()
    key = data.get('key')
    value = data.get('value')
    
    if key is None or value is None:
        return jsonify({"error": "Invalid data"}), 400
    
    # --- Mulai 2-Phase Commit ---
    print("\n--- Memulai 2-Phase Commit ---")
    # Fase 1: Mengirim PREPARE ke semua slave dan mengumpulkan suara
    votes = []
    for slave in SLAVE_SERVERS:
        try:
            print(f"Mengirim PREPARE ke {slave}")
            response = requests.post(f"{slave}/prepare", json={"key": key, "value": value}, timeout=1)
            if response.status_code == 200 and response.json().get("vote") == "COMMIT":
                votes.append("COMMIT")
                print(f"Menerima suara COMMIT dari {slave}")
            else:
                votes.append("ABORT")
                print(f"Menerima suara ABORT atau respons tidak valid dari {slave}")
        except requests.exceptions.RequestException as e:
            print(f"Gagal menghubungi {slave} untuk PREPARE: {e}")
            votes.append("ABORT")
    
    # Fase 2: Keputusan COMMIT atau ABORT
    if "ABORT" not in votes and len(votes) == len(SLAVE_SERVERS):
        # Jika semua setuju, kirim COMMIT
        print("--- Keputusan: COMMIT ---")
        for slave in SLAVE_SERVERS:
            try:
                requests.post(f"{slave}/commit", timeout=1)
                print(f"Mengirim COMMIT ke {slave}")
            except requests.exceptions.RequestException:
                print(f"Gagal mengirim COMMIT ke {slave}, data mungkin inkonsisten!") # Ini adalah kelemahan 2PC
        # Simpan data di master HANYA JIKA semua setuju
        master_data[key] = value
        print("--- Transaksi Berhasil ---")
        return jsonify({"message": "Transaction committed successfully"}), 200
    else:
        # Jika ada yang menolak, kirim ABORT
        print("--- Keputusan: ABORT ---")
        for slave in SLAVE_SERVERS:
            try:
                requests.post(f"{slave}/abort", timeout=1)
                print(f"Mengirim ABORT ke {slave}")
            except requests.exceptions.RequestException:
                pass # Abaikan error saat abort
        print("--- Transaksi Dibatalkan ---")
        return jsonify({"message": "Transaction aborted"}), 500

@app.route('/get/<key>', methods=['GET'])
def get_data(key):
    """Endpoint untuk membaca data dari master."""
    if key in master_data:
        return jsonify({key: master_data[key]}), 200
    else:
        return jsonify({"error": "Data not found"}), 404

@app.route('/sync', methods=['GET'])
def sync_data():
    """Endpoint untuk slave agar bisa menyalin seluruh data master."""
    print("Full data sync requested by a slave.")
    return jsonify(master_data), 200

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint untuk memeriksa status server."""
    return "Master is running", 200

if __name__ == '__main__':
    print("Master server running on port 5000")
    app.run(port=5000, debug=True, use_reloader=False)

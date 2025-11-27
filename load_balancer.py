# load_balancer.py
from flask import Flask, request
import requests
import itertools

# Inisialisasi aplikasi Flask untuk load balancer
app = Flask(__name__)

# Daftar server aplikasi (slave) yang akan menerima beban
SLAVE_SERVERS = [
    "http://127.0.0.1:5001",
    "http://127.0.0.1:5002",
    "http://127.0.0.1:5003",
    "http://127.0.0.1:5004"
]

# Membuat iterator untuk algoritma Round Robin (bergiliran)
# Ini akan terus berputar dari 5001 -> 5002 -> 5003 -> 5001 -> ...
server_iterator = itertools.cycle(SLAVE_SERVERS)

def get_healthy_server():
    """Mencari server yang sehat menggunakan Round Robin."""
    for _ in range(len(SLAVE_SERVERS)):
        server = next(server_iterator)
        try:
            # Kirim permintaan health check dengan timeout singkat
            response = requests.get(f"{server}/health", timeout=0.5)
            if response.status_code == 200:
                print(f"Server {server} sehat, meneruskan permintaan...")
                return server
        except requests.RequestException:
            print(f"Server {server} tidak merespons, mencari server lain...")
    return None # Jika tidak ada server yang sehat

@app.route('/get/<key>')
def forward_to_slave(key):
    """Menerima permintaan baca dan meneruskannya ke salah satu slave."""
    # 1. Pilih server slave tujuan yang sehat
    target_slave = get_healthy_server()
    
    if not target_slave:
        return "<h1>Tidak ada server slave yang tersedia</h1>", 503

    # 2. Teruskan permintaan ke slave yang dipilih
    try:
        resp = requests.get(f"{target_slave}/get/{key}")
        # Mengembalikan respons dari slave ke klien
        return (resp.content, resp.status_code, resp.headers.items())
    except requests.RequestException as e:
        print(f"Gagal meneruskan permintaan ke {target_slave}: {e}")
        return "<h1>Gagal memproses permintaan</h1>", 500

if __name__ == '__main__':
    print("Menjalankan Load Balancer di http://127.0.0.1:8080")
    app.run(port=8080)
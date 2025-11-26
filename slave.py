# slave.py
from flask import Flask, request, jsonify
import sys

# Inisialisasi aplikasi Flask
app = Flask(__name__)

# Penyimpanan data di memori (menggunakan dictionary)
slave_data = {}

# Mendapatkan port dari argumen command line, default 5001
port = int(sys.argv[1]) if len(sys.argv) > 1 else 5001

@app.route('/get/<key>', methods=['GET'])
def get_data(key):
    """Endpoint untuk membaca data dari slave."""
    if key in slave_data:
        return jsonify({key: slave_data[key]}), 200
    else:
        return jsonify({"error": "Data not found"}), 404

@app.route('/replicate', methods=['POST'])
def replicate_data():
    """Endpoint internal untuk menerima data dari master."""
    data = request.get_json()
    key = data.get('key')
    value = data.get('value')
    
    if key is None or value is None:
        return jsonify({"error": "Invalid replication data"}), 400
        
    # Simpan atau perbarui data
    slave_data[key] = value
    print(f"Data replicated on slave (Port {port}): {key} = {value}")
    return jsonify({"message": "Data replicated successfully"}), 200

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint untuk memeriksa status server."""
    return "Slave is running", 200

if __name__ == '__main__':
    print(f"Slave server running on port {port}")
    app.run(port=port, debug=True, use_reloader=False)


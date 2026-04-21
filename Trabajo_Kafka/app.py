import json
import threading
from flask import Flask, render_template, request, jsonify
from kafka import KafkaConsumer, KafkaProducer
from collections import defaultdict

app = Flask(__name__)

# Almacén en memoria de transacciones: id -> dict
transactions = {}
lock = threading.Lock()

# Productor Kafka para decisiones
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

# Consumidor de transacciones (topic 'transferencia')
def consume_transactions():
    consumer = KafkaConsumer(
        'transferencia',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        group_id='dashboard-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    for msg in consumer:
        tx = msg.value
        tx_id = tx['transaction_id']
        with lock:
            if tx_id not in transactions:
                transactions[tx_id] = tx
        print(f"Transacción recibida: {tx_id} - {tx['nombre']}")

# Iniciar hilo consumidor
threading.Thread(target=consume_transactions, daemon=True).start()

# API: listar transacciones
@app.route('/api/transactions')
def get_transactions():
    with lock:
        return jsonify(list(transactions.values()))

# API: aceptar/rechazar
@app.route('/api/decide/<tx_id>', methods=['POST'])
def decide(tx_id):
    data = request.get_json()
    action = data.get('action')  # 'accept' or 'reject'
    if action not in ('accept', 'reject'):
        return jsonify({'error': 'acción inválida'}), 400

    with lock:
        tx = transactions.get(tx_id)
        if not tx:
            return jsonify({'error': 'transacción no encontrada'}), 404
        if tx['estado'] != 'pendiente':
            return jsonify({'error': 'ya fue procesada'}), 400

        # Actualizar estado local
        new_status = 'aceptada' if action == 'accept' else 'rechazada'
        tx['estado'] = new_status

    # Enviar decisión al topic 'decisiones'
    decision_msg = {
        'transaction_id': tx_id,
        'accion': action,
        'entidad': tx['entidad'],
        'nombre': tx['nombre'],
        'monto': tx['monto']
    }
    producer.send('decisiones', value=decision_msg)
    producer.flush()

    return jsonify({'status': 'ok', 'nuevo_estado': new_status})

# Ruta principal
@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
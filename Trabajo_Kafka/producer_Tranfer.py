import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer

# Conexión a Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

TOPIC = "transferencia"

ENTIDADES = ['Banco Caja social', 'Banco Davivienda', 'Bancolombia','Banco Colpatria', 'Banco de Bogotá']
TIPOS_TRANSACCION = ['Transferencia', 'Pago de servicios', 'Retiro en cajero', 'Depósito en efectivo']
NOMBRES = ['Carlos López', 'Ana García', 'Juan Pérez', 'María Rodríguez', 'Luis Fernández', 'Laura Martínez']

print(f"...Productor de transacciones corriendo... (topic: {TOPIC})")

while True:
    evento = {
        'transaction_id': str(uuid.uuid4())[:8],
        'fecha': datetime.now().strftime('%Y-%m-%d'),
        'hora': datetime.now().strftime('%H:%M:%S'),
        'nombre': random.choice(NOMBRES),
        'entidad': random.choice(ENTIDADES),
        'tipo': random.choice(TIPOS_TRANSACCION),
        'monto': round(random.uniform(10.0, 5000.0), 2),
        'estado': 'pendiente'
    }

    producer.send(TOPIC, value=evento)
    print(f"✉ [ENVIADO] {evento['transaction_id']} | {evento['nombre']} | {evento['entidad']} | ${evento['monto']}")
    time.sleep(30)  # una transacción cada 10 segundos
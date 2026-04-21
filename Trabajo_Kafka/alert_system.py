import json
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime

# Consumidor del topic 'transferencia'
consumer = KafkaConsumer(
    'transferencia',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='alerta-group',      # Grupo independiente
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Productor para el topic 'alertas'
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

print("🚨 Sistema de Alertas activo. Evaluando transacciones menores a $1000...")

for msg in consumer:
    tx = msg.value
    monto = tx['monto']
    if monto < 1000:
        alerta = {
            'transaction_id': tx['transaction_id'],
            'monto': monto,
            'nombre': tx['nombre'],
            'entidad': tx['entidad'],
            'tipo': tx['tipo'],
            'timestamp_alerta': datetime.now().isoformat(),
            'razon': 'Monto bajo (menor a $1000)',
            'severidad': 'informativa'
        }
        producer.send('alertas', value=alerta)
        producer.flush()
        print(f"⚠️ ALERTA: Transacción {tx['transaction_id']} de ${monto} - {tx['nombre']}")
    else:
        print(f"✅ Transacción normal: {tx['transaction_id']} - ${monto}")
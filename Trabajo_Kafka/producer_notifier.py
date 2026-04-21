import json
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime

# Consumidor del topic 'decisiones'
consumer = KafkaConsumer(
    'decisiones',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='notifier-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Productor para notificaciones
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

print("Servicio de notificaciones activo (escuchando decisiones)...")

for msg in consumer:
    decision = msg.value
    trans_id = decision['transaction_id']
    accion = decision['accion']  # 'accept' o 'reject'
    estado_final = "aceptada" if accion == "accept" else "rechazada"

    notificacion = {
        'transaction_id': trans_id,
        'entidad': decision['entidad'],
        'destinatario': decision['nombre'],
        'estado': estado_final,
        'mensaje': f"Transacción {trans_id} ha sido {estado_final}",
        'timestamp': datetime.now().isoformat()
    }

    producer.send('notificacion', value=notificacion)
    print(f"Notificación enviada: {notificacion}")
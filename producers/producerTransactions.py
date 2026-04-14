#       Importacion librerias 
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
import uuid

#1 (configuracion de kafka)
KAFKA_BROKER= 'kafka:9092'      #Direccion del servidor kafka
Topic= 'transactions'

producer= KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')    #Transformacion de JSON a bytes para enviar a kafka
)

#2 (Datos randoms para las transacciones)
CUSTOMERS= ['Alice', 'Bob', 'Charlie', 'David', 'Eve']
ENTITIES= ['Banco Caja social', 'Banco Davivienda', 'Bancolombia', 'Banco Colpatria', 'Banco de Bogotá']
TYPES= ['Transferencia', 'Pago de servicios', 'Retiro en cajero', 'Depósito en efectivo']

def generate_transaction():
    return {
        'id': str(uuid.uuid4()),        #ID unico para cada transaccion
        'customer': random.choice(CUSTOMERS),
        'Source_entity': random.choice(ENTITIES),
        'Destination_entity': random.choice(ENTITIES),
        'transaction_type': random.choice(TYPES),
        'amount': round(random.uniform(10.0, 1000.0), 2),   #Monto de dinero aleatorio
        'timestamp': datetime.now().isoformat()     #fecha y hora actual
    }

print("Se esta enviando la trasaccion a kafka...")
while True:
    tx= generate_transaction()
    producer.send(Topic, value=tx)
    print(f"Transacción enviada: {tx}")
    time.sleep(1)  # transaccion por segundo (1)

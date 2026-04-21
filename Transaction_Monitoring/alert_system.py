import os
import json
from datetime import datetime
from kafka import KafkaConsumer
import sqlite3

KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC_ESTADISTICAS = 'estadisticas_ventana'
SQLITE_DB = '/app/sqlite_db/transacciones.db'
UMBRAL_RECHAZOS = 5

# Crear consumidor Kafka
consumer = KafkaConsumer(
    TOPIC_ESTADISTICAS,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Sistema de Alertas iniciado. Esperando estadísticas de ventana...")

for msg in consumer:
    stats = msg.value
    rechazados = stats.get('rechazados_ventana', 0)
    if rechazados > UMBRAL_RECHAZOS:
        # Generar alerta
        alerta = {
            'ventana_inicio': stats['ventana_inicio'],
            'ventana_fin': stats['ventana_fin'],
            'rechazados': rechazados,
            'timestamp_deteccion': datetime.now().isoformat()
        }
        # Guardar en SQLite
        conn = sqlite3.connect(SQLITE_DB)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS alertas (
                ventana_inicio TEXT,
                ventana_fin TEXT,
                rechazados INTEGER,
                timestamp_deteccion TEXT
            )
        ''')
        cursor.execute('''
            INSERT INTO alertas (ventana_inicio, ventana_fin, rechazados, timestamp_deteccion)
            VALUES (?, ?, ?, ?)
        ''', (alerta['ventana_inicio'], alerta['ventana_fin'], alerta['rechazados'], alerta['timestamp_deteccion']))
        conn.commit()
        conn.close()
        print(f"ALERTA: {rechazados} rechazos en ventana {alerta['ventana_inicio']} - {alerta['ventana_fin']}")
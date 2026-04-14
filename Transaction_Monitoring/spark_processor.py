import os
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, min, max, count, sum, when, lit, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import sqlite3

# Configuración
KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
INPUT_TOPIC = 'transacciones'
OUTPUT_TOPIC = 'decisiones'
CHECKPOINT_DIR = '/tmp/spark_checkpoint'
CSV_OUTPUT = '/app/csv_output/estadisticas_ventana.csv'
SQLITE_DB = '/app/sqlite_db/transacciones.db'

UMBRAL_MONTO = 10000.0
UMBRAL_RECHAZOS_VENTANA = 5

# Crear SparkSession
spark = SparkSession.builder \
    .appName("MonitorTransacciones") \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Esquema de la transacción
esquema_transaccion = StructType([
    StructField("id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("cliente", StringType(), True),
    StructField("entidad_origen", StringType(), True),
    StructField("entidad_destino", StringType(), True),
    StructField("tipo", StringType(), True),
    StructField("monto", DoubleType(), True)
])

# Leer stream de Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", INPUT_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Parsear JSON
transacciones = raw_stream.select(
    from_json(col("value").cast("string"), esquema_transaccion).alias("data")
).select("data.*")

# Convertir timestamp a tipo Timestamp
transacciones = transacciones.withColumn(
    "event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
)

# Asignar estado: Rechazada si monto > umbral, sino Aceptada
transacciones_con_estado = transacciones.withColumn(
    "estado",
    when(col("monto") > UMBRAL_MONTO, lit("Rechazada")).otherwise(lit("Aceptada"))
)

# Función para guardar cada transacción en SQLite
def guardar_en_sqlite(df, epoch_id):
    pdf = df.select("id", "timestamp", "cliente", "entidad_origen", "entidad_destino",
                    "tipo", "monto", "estado").toPandas()
    if pdf.empty:
        return
    conn = sqlite3.connect(SQLITE_DB)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transacciones (
            id TEXT PRIMARY KEY,
            timestamp TEXT,
            cliente TEXT,
            entidad_origen TEXT,
            entidad_destino TEXT,
            tipo TEXT,
            monto REAL,
            estado TEXT
        )
    ''')
    for _, row in pdf.iterrows():
        cursor.execute('''
            INSERT OR REPLACE INTO transacciones (id, timestamp, cliente, entidad_origen,
                entidad_destino, tipo, monto, estado)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', tuple(row))
    conn.commit()
    conn.close()

# Función para enviar decisiones a Kafka (para el dashboard)
def enviar_decisiones_kafka(df, epoch_id):
    decisiones = df.select("id", "timestamp", "tipo", "monto", "entidad_origen", "estado")
    decisiones.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", OUTPUT_TOPIC) \
        .mode("append") \
        .save()

# Escribir stream a SQLite y Kafka
query_sqlite = transacciones_con_estado.writeStream \
    .foreachBatch(guardar_en_sqlite) \
    .outputMode("append") \
    .start()

query_kafka = transacciones_con_estado.writeStream \
    .foreachBatch(enviar_decisiones_kafka) \
    .outputMode("append") \
    .start()

# ===== Ventana tumbling de 1 minuto con estadísticas =====
ventana_stats = transacciones_con_estado \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(window(col("event_time"), "1 minute")) \
    .agg(
        count("*").alias("total_transacciones"),
        sum("monto").alias("monto_total"),
        avg("monto").alias("monto_promedio"),
        min("monto").alias("monto_minimo"),
        max("monto").alias("monto_maximo"),
        count(when(col("estado") == "Rechazada", 1)).alias("rechazados_ventana")
    )

# Detectar alertas: ventanas con muchos rechazos
alertas = ventana_stats.filter(col("rechazados_ventana") > UMBRAL_RECHAZOS_VENTANA)

# Función para guardar estadísticas en CSV y alertas en SQLite
def guardar_estadisticas_y_alertas(df, epoch_id):
    # Guardar estadísticas en CSV (modo append)
    df.write.mode("append").option("header", "true").csv(CSV_OUTPUT)
    
    # Guardar alertas en SQLite
    pdf_alerts = df.select("window", "rechazados_ventana").toPandas()
    if not pdf_alerts.empty:
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
        for _, row in pdf_alerts.iterrows():
            cursor.execute('''
                INSERT INTO alertas (ventana_inicio, ventana_fin, rechazados, timestamp_deteccion)
                VALUES (?, ?, ?, ?)
            ''', (str(row['window'].start), str(row['window'].end), row['rechazados_ventana'],
                  datetime.now().isoformat()))
        conn.commit()
        conn.close()

query_stats = ventana_stats.writeStream \
    .foreachBatch(guardar_estadisticas_y_alertas) \
    .outputMode("append") \
    .start()

# Esperar a que terminen todas las queries
query_sqlite.awaitTermination()
query_kafka.awaitTermination()
query_stats.awaitTermination()
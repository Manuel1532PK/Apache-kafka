import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, min as spark_min, max as spark_max, count, sum as spark_sum, when, lit, to_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import sqlite3

# 1. (Configuración)
KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
INPUT_TOPIC = 'transactions'
OUTPUT_TOPIC = 'decisions'
CHECKPOINT_DIR = '/tmp/spark_checkpoint'
CSV_OUTPUT = '/app/csv_output/window_stats.csv'
SQLITE_DB = '/app/sqlite_db/transactions.db'

# 2. (Umbrales)
AMOUNT_THRESHOLD = 10000.0
REJECTS_WINDOW_THRESHOLD = 5

# 3. (Crear Spark Session)

spark = SparkSession.builder \
    .appName("TransactionMonitor") \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 4. (Esquema de la transacción)

tx_schema = StructType([
    StructField("id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("customer", StringType(), True),
    StructField("source_entity", StringType(), True),
    StructField("destination_entity", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("amount", DoubleType(), True)
])

# 5. (Leer stream de Kafka)

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", INPUT_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# 6. (Parsear JSON)

parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), tx_schema).alias("data")
).select("data.*")

# 7. (Conversión timestamp a tipo timestamp)
parsed_stream = parsed_stream.withColumn(
    "event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
)


enriched_stream = parsed_stream.withColumn(
    "status",
    when(col("amount") > AMOUNT_THRESHOLD, lit("Rechazada")).otherwise(lit("Aceptada"))
)

# 8. (Función para guardar cada transacción en SQLite (foreachBatch))
def save_to_sqlite(df, epoch_id):

    # Convertir a pandas para facilitar inserción
    pdf = df.select("id", "timestamp", "customer", "source_entity", "destination_entity",
                    "transaction_type", "amount", "status").toPandas()
    if pdf.empty:
        return
    conn = sqlite3.connect(SQLITE_DB)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            id TEXT PRIMARY KEY,
            timestamp TEXT,
            customer TEXT,
            source_entity TEXT,
            destination_entity TEXT,
            transaction_type TEXT,
            amount REAL,
            status TEXT
        )
    ''')
    # Insertar o ignorar duplicados
    for _, row in pdf.iterrows():
        cursor.execute('''
            INSERT OR REPLACE INTO transactions (id, timestamp, customer, source_entity,
                destination_entity, transaction_type, amount, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', tuple(row))
    conn.commit()
    conn.close()

# 8. (Enviar decisiones a Kafka dashboard)
def send_decisions_to_kafka(df, epoch_id):
    """Envía cada transacción con estado al topic 'decisions'"""
    decisions = df.select("id", "timestamp", "transaction_type", "amount", "source_entity", "status")
    decisions.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", OUTPUT_TOPIC) \
        .mode("append") \
        .save()

# 9. (Escribir stream con foreachBatch para múltiples salidas)
query1 = enriched_stream.writeStream \
    .foreachBatch(lambda df, epoch: save_to_sqlite(df, epoch)) \
    .outputMode("append") \
    .start()

query2 = enriched_stream.writeStream \
    .foreachBatch(lambda df, epoch: send_decisions_to_kafka(df, epoch)) \
    .outputMode("append") \
    .start()

# 10. (Ventana Tumbling de 1 minuto: estadísticas y alertas)
windowed_stats = enriched_stream \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(
        window(col("event_time"), "1 minute")
    ) \
    .agg(
        count("*").alias("total_transactions"),
        spark_sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount"),
        spark_min("amount").alias("min_amount"),
        spark_max("amount").alias("max_amount"),
        count(when(col("status") == "Rechazada", 1)).alias("rejected_count")
    )

# 11. (Detectar alertas: si rejected_count > umbral)
alert_stream = windowed_stats.filter(col("rejected_count") > REJECTS_WINDOW_THRESHOLD)

# 12. (Función para guardar estadísticas en CSV y alertas en SQLite)
def save_stats_and_alerts(df, epoch_id):
    # Guardar estadísticas en CSV (modo append)
    df.write.mode("append").option("header", "true").csv(CSV_OUTPUT)
    
    # Guardar alertas en SQLite
    pdf = df.select("window", "rejected_count").toPandas()
    if pdf.empty:
        return
    conn = sqlite3.connect(SQLITE_DB)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS alerts (
            window_start TEXT,
            window_end TEXT,
            rejected_count INTEGER,
            timestamp TEXT
        )
    ''')
    for _, row in pdf.iterrows():
        cursor.execute('''
            INSERT INTO alerts (window_start, window_end, rejected_count, timestamp)
            VALUES (?, ?, ?, ?)
        ''', (str(row['window'].start), str(row['window'].end), row['rejected_count'], 
              datetime.now().isoformat()))
    conn.commit()
    conn.close()

# 13. (Escribir estadísticas y alertas)
query_stats = windowed_stats.writeStream \
    .foreachBatch(lambda df, epoch: save_stats_and_alerts(df, epoch)) \
    .outputMode("append") \
    .start()

# 14. (Esperar a que terminen todas las queries)
query1.awaitTermination()
query2.awaitTermination()
query_stats.awaitTermination()
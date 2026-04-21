import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, min, max, sum, count, when, lit, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import sqlite3

KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'host.docker.internal:9092')
INPUT_TOPIC = 'transacciones'
OUTPUT_TOPIC_DECISIONES = 'decisiones'
CSV_OUTPUT = '/app/csv_output/estadisticas_ventana.csv'
SQLITE_DB = '/app/sqlite_db/transacciones.db'
UMBRAL_MONTO = 10000.0

spark = SparkSession.builder.appName("MonitorTransacciones").config("spark.sql.shuffle.partitions", "2").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("id", StringType()) \
    .add("timestamp", StringType()) \
    .add("cliente", StringType()) \
    .add("entidad_origen", StringType()) \
    .add("entidad_destino", StringType()) \
    .add("tipo", StringType()) \
    .add("monto", DoubleType())

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", INPUT_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

parsed = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss")) \
    .withColumn("estado", when(col("monto") > UMBRAL_MONTO, lit("Rechazada")).otherwise(lit("Aceptada")))

def guardar_transacciones(df, epoch_id):
    pdf = df.select("id", "timestamp", "cliente", "entidad_origen", "entidad_destino", "tipo", "monto", "estado").toPandas()
    if pdf.empty:
        return
    conn = sqlite3.connect(SQLITE_DB)
    cursor = conn.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS transacciones (id TEXT PRIMARY KEY, timestamp TEXT, cliente TEXT, entidad_origen TEXT, entidad_destino TEXT, tipo TEXT, monto REAL, estado TEXT)')
    for _, row in pdf.iterrows():
        cursor.execute('INSERT OR REPLACE INTO transacciones VALUES (?,?,?,?,?,?,?,?)', tuple(row))
    conn.commit()
    conn.close()

def enviar_decisiones(df, epoch_id):
    df.select("id", "timestamp", "tipo", "monto", "entidad_origen", "estado") \
      .write.format("kafka") \
      .option("kafka.bootstrap.servers", KAFKA_BROKER) \
      .option("topic", OUTPUT_TOPIC_DECISIONES) \
      .mode("append") \
      .save()

def guardar_estadisticas(df, epoch_id):
    df.write.mode("append").option("header", "true").csv(CSV_OUTPUT)

ventana_stats = parsed \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(window(col("event_time"), "1 minute")) \
    .agg(
        count("*").alias("total_transacciones"),
        sum("monto").alias("monto_total"),
        avg("monto").alias("monto_promedio"),
        min("monto").alias("monto_minimo"),
        max("monto").alias("monto_maximo"),
        count(when(col("estado") == "Rechazada", 1)).alias("rechazados_ventana")
    ) \
    .select(
        col("window.start").alias("ventana_inicio"),
        col("window.end").alias("ventana_fin"),
        "total_transacciones", "monto_total", "monto_promedio",
        "monto_minimo", "monto_maximo", "rechazados_ventana"
    )

q1 = parsed.writeStream.foreachBatch(guardar_transacciones).outputMode("append").start()
q2 = parsed.writeStream.foreachBatch(enviar_decisiones).outputMode("append").start()
q3 = ventana_stats.writeStream.foreachBatch(guardar_estadisticas).outputMode("append").start()

q1.awaitTermination()
q2.awaitTermination()
q3.awaitTermination()
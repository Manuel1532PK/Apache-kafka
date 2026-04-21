import os
import time
from flask import Flask, render_template, Response
from kafka import KafkaConsumer
import json
import threading

app = Flask(__name__)
KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC_DECISIONES = 'decisiones'

consumer = None
lock = threading.Lock()

def init_consumer():
    global consumer
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_DECISIONES,
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print("Dashboard conectado a Kafka")
            break
        except Exception as e:
            print(f"Esperando Kafka... {e}")
            time.sleep(5)

threading.Thread(target=init_consumer, daemon=True).start()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/stream')
def stream():
    def event_stream():
        global consumer
        while True:
            if consumer is None:
                yield "data: {}\n\n".format(json.dumps({"info": "Conectando..."}))
                time.sleep(2)
                continue
            try:
                for msg in consumer:
                    yield f"data: {json.dumps(msg.value)}\n\n"
            except Exception as e:
                print(e)
                time.sleep(1)
    return Response(event_stream(), mimetype="text/event-stream")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
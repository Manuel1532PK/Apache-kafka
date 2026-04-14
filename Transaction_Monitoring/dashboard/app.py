import os
from flask import Flask, render_template, Response
from kafka import KafkaConsumer
import json

app = Flask(__name__)
KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC_DECISIONES = 'decisiones'

consumer = KafkaConsumer(
    TOPIC_DECISIONES,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/stream')
def stream():
    def event_stream():
        for msg in consumer:
            tx = msg.value
            yield f"data: {json.dumps(tx)}\n\n"
    return Response(event_stream(), mimetype="text/event-stream")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, threaded=True)
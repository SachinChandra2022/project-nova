import json
from threading import Thread
from flask import Flask, render_template
from flask_socketio import SocketIO
from kafka import KafkaConsumer

# --- Flask and SocketIO Setup ---
app = Flask(__name__)
# A secret key is needed for session management, even if we don't use sessions.
app.config['SECRET_KEY'] = 'your_secret_key_here!'
socketio = SocketIO(app)

# --- Kafka Configuration ---
KAFKA_BROKER_URL = "localhost:29092"
TELEMETRY_TOPIC = "drone-telemetry"
ALERTS_TOPIC = "drone-alerts"

# --- Kafka Consumer Threads ---
# We need to run our Kafka consumers in background threads so they don't
# block the web server.

def telemetry_consumer():
    """Consumes telemetry data and emits it via WebSocket."""
    consumer = KafkaConsumer(
        TELEMETRY_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        group_id="dashboard-telemetry-group", # A new group for the dashboard
        value_deserializer=lambda value: json.loads(value.decode('utf-8'))
    )
    for message in consumer:
        # Emit the data to all connected web clients
        socketio.emit('update_telemetry', message.value)
        print(f"Sent telemetry: {message.value}") # For debugging

def alerts_consumer():
    """Consumes alert data and emits it via WebSocket."""
    consumer = KafkaConsumer(
        ALERTS_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        group_id="dashboard-alerts-group", # A new group for the dashboard
        value_deserializer=lambda value: json.loads(value.decode('utf-8'))
    )
    for message in consumer:
        # Emit the data to all connected web clients
        socketio.emit('new_alert', message.value)
        print(f"Sent alert: {message.value}") # For debugging

# --- Flask Routes ---
@app.route('/')
def index():
    """Serves the main dashboard page."""
    return render_template('index.html')

# --- Main Application ---
if __name__ == '__main__':
    print("Starting Kafka consumer threads...")
    telemetry_thread = Thread(target=telemetry_consumer, daemon=True)
    alerts_thread = Thread(target=alerts_consumer, daemon=True)
    telemetry_thread.start()
    alerts_thread.start()

    print("Starting web server...")
    # The web server runs in the main thread
    socketio.run(app, debug=True, use_reloader=False)
import json
from kafka import KafkaConsumer

# --- Configuration ---
KAFKA_BROKER_URL = "localhost:29092"
TELEMETRY_TOPIC = "drone-telemetry"

def main():
    # Create a Kafka consumer
    consumer = KafkaConsumer(
        TELEMETRY_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        # This is CRITICAL for scaling. All instances of this command center
        # will join the same group and Kafka will distribute messages among them.
        group_id="command-center-group",
        # Automatically deserialize the JSON message from bytes.
        value_deserializer=lambda value: json.loads(value.decode('utf-8'))
    )

    print("Command Center is online. Listening for drone telemetry...")

    # The consumer is an iterator that will block and wait for new messages.
    for message in consumer:
        # message.value is the deserialized dictionary
        telemetry_data = message.value
        drone_id = telemetry_data.get("drone_id")
        battery = telemetry_data.get("battery")
        
        # Simple processing: print a formatted status update
        print(f"[STATUS] Drone {drone_id}: Battery at {battery}%")

        # In a real system, you might do more here:
        # - Store the data in a database
        # - Push it to a WebSocket for a live dashboard
        # - Check for anomalies

if __name__ == "__main__":
    main()
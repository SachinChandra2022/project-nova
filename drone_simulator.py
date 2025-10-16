import time
import json
import random
from kafka import KafkaProducer

# --- Configuration ---
# The address of your Kafka broker, accessible from your host machine.
KAFKA_BROKER_URL = "localhost:29092"
# The topic where the drone will send its telemetry data.
TELEMETRY_TOPIC = "drone-telemetry"
# A unique ID for our drone.
DRONE_ID = f"nova-{random.randint(100, 999)}"

# --- Drone Simulation ---
# Starting position (Lucknow, India)
latitude = 26.8467
longitude = 80.9462
altitude = 100.0
battery = 100.0
status = "IN_FLIGHT"

def create_producer():
    """Creates and returns a Kafka producer."""
    try:
        # We serialize our dictionary message to a JSON string, then encode it to bytes.
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_URL,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Kafka Producer connected successfully.")
        return producer
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        return None

def simulate_drone_movement():
    """Simulates the drone's movement and status changes."""
    global latitude, longitude, altitude, battery, status

    # Simulate slight random movement
    latitude += random.uniform(-0.0005, 0.0005)
    longitude += random.uniform(-0.0005, 0.0005)
    
    # Simulate battery drain
    battery -= 0.05
    if battery < 0:
        battery = 0
        
    # Check for low battery status
    if battery < 20 and status != "LOW_BATTERY":
        status = "LOW_BATTERY"
    elif battery >= 20 and status == "LOW_BATTERY":
        status = "IN_FLIGHT"

def main():
    producer = create_producer()
    if not producer:
        return

    print(f"Launching drone {DRONE_ID}...")

    while True:
        # Update the drone's position and status
        simulate_drone_movement()

        # Prepare the telemetry data
        telemetry_data = {
            "drone_id": DRONE_ID,
            "latitude": round(latitude, 6),
            "longitude": round(longitude, 6),
            "altitude": round(altitude, 1),
            "battery": round(battery, 2),
            "status": status,
            "timestamp": int(time.time() * 1000) # milliseconds
        }

        try:
            # Send the data to the Kafka topic
            producer.send(TELEMETRY_TOPIC, telemetry_data)

            # Print to our console to see what's happening
            print(f"Sent: {telemetry_data}")
        
        except Exception as e:
            print(f"Error sending message: {e}")

        # Wait for a second before sending the next update
        time.sleep(1)

if __name__ == "__main__":
    main()
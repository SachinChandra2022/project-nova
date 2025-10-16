import json
from kafka import KafkaConsumer, KafkaProducer

# --- Configuration ---
KAFKA_BROKER_URL = "localhost:29092"
TELEMETRY_TOPIC = "drone-telemetry"
ALERTS_TOPIC = "drone-alerts"

# Define a simple geographical boundary (geofence) for Lucknow
GEOFENCE = {
    "lat_min": 26.75,
    "lat_max": 26.95,
    "lon_min": 80.85,
    "lon_max": 81.05,
}

# --- State Management ---
# Keep track of drones with active alerts to avoid sending duplicate alerts
drones_with_low_battery = set()
drones_outside_geofence = set()


def create_producer():
    """Creates a Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def check_geofence(lat, lon):
    """Check if the drone is within the defined geofence."""
    if not (GEOFENCE["lat_min"] < lat < GEOFENCE["lat_max"] and
            GEOFENCE["lon_min"] < lon < GEOFENCE["lon_max"]):
        return False
    return True

def main():
    # This service is both a consumer and a producer
    producer = create_producer()
    
    consumer = KafkaConsumer(
        TELEMETRY_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        # IMPORTANT: Use a different group_id for this service
        # so it receives all messages from the telemetry topic independently
        # of the command_center service.
        group_id="alerting-service-group",
        value_deserializer=lambda value: json.loads(value.decode('utf-8'))
    )

    print("Alerting Service is online...")

    for message in consumer:
        telemetry_data = message.value
        drone_id = telemetry_data["drone_id"]
        battery = telemetry_data["battery"]
        lat = telemetry_data["latitude"]
        lon = telemetry_data["longitude"]

        # --- Rule 1: Low Battery Alert ---
        if battery < 20 and drone_id not in drones_with_low_battery:
            alert = {
                "drone_id": drone_id,
                "alert_type": "LOW_BATTERY",
                "message": f"Critical battery level at {battery}%!",
                "level": "CRITICAL"
            }
            producer.send(ALERTS_TOPIC, alert)
            drones_with_low_battery.add(drone_id) # Add to set to prevent re-alerting
            print(f"[ALERT] Sent LOW_BATTERY alert for {drone_id}")
        
        # If battery recharges, remove it from the set so it can be alerted again later
        elif battery >= 20 and drone_id in drones_with_low_battery:
            drones_with_low_battery.remove(drone_id)

        # --- Rule 2: Geofence Breach Alert ---
        if not check_geofence(lat, lon) and drone_id not in drones_outside_geofence:
            alert = {
                "drone_id": drone_id,
                "alert_type": "GEOFENCE_BREACH",
                "message": f"Drone has left the designated operational area!",
                "location": {"lat": lat, "lon": lon},
                "level": "WARNING"
            }
            producer.send(ALERTS_TOPIC, alert)
            drones_outside_geofence.add(drone_id)
            print(f"[ALERT] Sent GEOFENCE_BREACH alert for {drone_id}")

        elif check_geofence(lat, lon) and drone_id in drones_outside_geofence:
            drones_outside_geofence.remove(drone_id)


if __name__ == "__main__":
    main()
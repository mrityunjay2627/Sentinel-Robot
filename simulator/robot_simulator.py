import json
import boto3
import random
import time
import uuid

# --- Configuration ---
STREAM_NAME = "sentinel-telemetry-stream"
AWS_REGION = "us-east-1"
# ---------------------

# Create a Kinesis client
kinesis_client = boto3.client('kinesis', region_name=AWS_REGION)

def send_data(robot_id):
    """Generates and sends a single data record."""
    
    # Simulate sensor data
    data = {
        "robotId": robot_id,
        "timestamp": int(time.time()),
        "motorTemp": round(random.uniform(55.0, 75.0), 2), # Celsius
        "batteryVoltage": round(random.uniform(22.0, 24.5), 2), # Volts
        "vibration": round(random.uniform(0.1, 2.5), 3), # g-force
    }
    
    # Convert data to JSON string and then to bytes
    payload = json.dumps(data)
    print(f"Sending: {payload}")
    
    # Send to Kinesis
    kinesis_client.put_record(
        StreamName=STREAM_NAME,
        Data=payload.encode('utf-8'),
        PartitionKey=robot_id # Use robotId to distribute records among shards
    )

if __name__ == "__main__":
    robot_id_simulation = str(uuid.uuid4())
    print(f"Simulating data for robot: {robot_id_simulation}")
    
    while True:
        try:
            send_data(robot_id_simulation)
            time.sleep(5) # Send data every 5 seconds
        except KeyboardInterrupt:
            print("\nSimulation stopped.")
            break
        except Exception as e:
            print(f"An error occurred: {e}")
            time.sleep(10) # Wait before retrying
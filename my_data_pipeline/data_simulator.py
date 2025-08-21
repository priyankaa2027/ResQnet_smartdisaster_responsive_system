import boto3
import time
import random
from decimal import Decimal # To handle decimal numbers for DynamoDB
from botocore.exceptions import BotoCoreError, ClientError

# --- BOTO3 CONNECTION (SECURE) ---
try:
    dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')
    table = dynamodb.Table('sensor_logs') # CORRECTED: Using underscore
    table.load()
    print("[INFO] Successfully connected to DynamoDB table 'sensor_logs'.")
except Exception as e:
    print(f"[FATAL ERROR] Could not connect to DynamoDB. Check credentials/region. Error: {e}")
    exit()

# --- List of sensors to simulate ---
sensors = [
    "india-wildfire-sensor-UK01",
    "india-flood-sensor-KL01",
    "india-earthquake-sensor-NCR01"
]

print("--- Starting TEST Data Simulation with Randomly Generated Data ---")

# --- MAIN SIMULATION LOOP ---
# This loop will run forever. Press Ctrl+C in the terminal to stop it.
while True:
    try:
        current_sensor_id = random.choice(sensors)
        data_point = {}

        if "wildfire" in current_sensor_id:
            data_point = {
                'temperature': random.randint(20, 45),
                'humidity': random.randint(15, 90),
                'wind_speed': random.randint(5, 40)
            }
        elif "flood" in current_sensor_id:
            data_point = {
                'rainfall_mm': random.randint(0, 70),
                # FIXED: Convert the float to a string, then to a Decimal
                'river_level_m': Decimal(str(round(random.uniform(1.0, 5.0), 2)))
            }
        elif "earthquake" in current_sensor_id:
            data_point = {
                # FIXED: Convert the float to a string, then to a Decimal
                'seismic_intensity_mmf': Decimal(str(round(random.uniform(1.0, 6.0), 1))),
                'depth_km': random.randint(1, 50)
            }

        final_data_packet = {
            'sensorId': current_sensor_id,
            'timestamp': int(time.time()),
            **data_point
        }

        if data_point:
            table.put_item(Item=final_data_packet)
            print(f"[INFO] Sent data from {current_sensor_id}: {final_data_packet}")

    except (BotoCoreError, ClientError) as e:
        print(f"[ERROR] AWS Error sending data: {e}")
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred: {e}")

    print("[INFO] Waiting for 5 seconds...")
    time.sleep(5)
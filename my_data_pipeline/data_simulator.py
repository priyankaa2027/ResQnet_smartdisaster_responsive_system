import boto3
import time
import csv
import random # Import random for unique sensor IDs if needed

# Initialize the DynamoDB resource.
# Ensure your AWS credentials are configured via AWS CLI (aws configure)
# or environment variables. DO NOT hardcode them here for production.
dynamodb = boto3.resource('dynamodb', region_name='ap-south-1') # Use your preferred AWS region

# Get a reference to your DynamoDB table
table = dynamodb.Table('sensor-logs')

def simulate_event(filename, event_type, base_sensor_id):
    """
    Reads data from a CSV file and sends it to DynamoDB with a specified event type.

    Args:
        filename (str): The path to the CSV data file.
        event_type (str): The type of event (e.g., 'wildfire', 'earthquake', 'flood').
        base_sensor_id (str): A base ID for the sensor, to which a random number
                              will be appended for uniqueness.
    """
    print(f"\n--- Simulating {event_type} event from {filename} ---")
    
    with open(filename, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for i, row in enumerate(reader):
            # Generate a slightly unique sensorId for each data point within the simulation
            # This helps in distinguishing individual entries if 'sensorId' is part of the key
            # or if you want to track multiple "virtual" sensors of the same type.
            sensor_id = f"{base_sensor_id}-{random.randint(1000, 9999)}"
            
            data_point = {
                'sensorId': sensor_id,
                'timestamp': int(time.time()), # Current Unix timestamp
                'eventType': event_type
            }

            # Add specific metrics based on the event type
            if event_type == 'wildfire':
                # Ensure data types match DynamoDB expectations (e.g., numbers are numbers)
                data_point['temperature'] = int(row['temperature'])
                data_point['humidity'] = int(row['humidity'])
                data_point['wind_speed'] = int(row['wind_speed'])
            elif event_type == 'earthquake':
                data_point['magnitude'] = float(row['magnitude'])
                data_point['depth'] = float(row['depth'])
                data_point['location'] = row['location'] # String type
            elif event_type == 'flood':
                data_point['water_level'] = float(row['water_level'])
                data_point['rainfall'] = float(row['rainfall'])
                data_point['flow_rate'] = float(row['flow_rate'])
            else:
                print(f"Warning: Unknown event type '{event_type}'. Data might not be fully added.")
                continue # Skip to the next row if event type is not recognized

            try:
                # Put the item into the DynamoDB table
                table.put_item(Item=data_point)
                print(f"Sent: {data_point}")
            except Exception as e:
                print(f"Error sending data for {event_type} (Sensor ID: {sensor_id}): {e}")

            time.sleep(2) # Wait 2 seconds before sending the next data point

# --- Main simulation execution ---
if __name__ == "__main__":
    # Simulate wildfire events
    simulate_event('wildfire_data.csv', 'wildfire', 'wildfire-sensor')
    
    # Simulate earthquake events
    simulate_event('earthquake_data.csv', 'earthquake', 'earthquake-sensor')

    # Simulate flood events
    simulate_event('flood_data.csv', 'flood', 'flood-sensor')

    print("\nAll simulations completed.")


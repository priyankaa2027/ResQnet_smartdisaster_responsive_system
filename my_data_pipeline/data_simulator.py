import boto3
import json
from boto3.dynamodb.conditions import Key
from decimal import Decimal

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')
    table = dynamodb.Table('sensor_logs')

    # Get the latest record
    response = table.scan()
    items = sorted(response['Items'], key=lambda x: x['timestamp'], reverse=True)

    if not items:
        return {
            'statusCode': 404,
            'body': json.dumps({'error': 'No data found'})
        }

    latest = items[0]

    # Prediction logic (dummy)
    prediction = "SEVERE" if 'temperature' in latest and latest['temperature'] > 40 else "NORMAL"

    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps({
            'data': {
                'temperature': int(latest.get('temperature', 0)),
                'humidity': int(latest.get('humidity', 0)),
                'windSpeed': int(latest.get('wind_speed', 0))
            },
            'prediction': prediction
        })
    }

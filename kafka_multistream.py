from confluent_kafka import Consumer, KafkaException
import json
import time
import csv
import os
from datetime import datetime, timezone, timedelta

# Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'latency-group',
    'auto.offset.reset': 'earliest'
}

# Create Kafka consumer and subscribe to topics
consumer = Consumer(consumer_conf)
consumer.subscribe(['sensor1-data', 'sensor2-data'])

# Set log export directory
output_dir = "./kafka_logs"

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

output_csv = os.path.join(output_dir, "failed_log.csv")

# To match the time zone format in Spark log
set_timezone = timezone(timedelta(hours=11))

def calculate_latency(upstream_event_timestamp):
    kafka_consumer_timestamp = int(time.time())  # Same format as live stream timestamp
    latency = (kafka_consumer_timestamp - upstream_event_timestamp) * 1000  # Latency in milliseconds

    return kafka_consumer_timestamp, latency

# To match the time zone format in Spark log
def iso8601_format(timestamp):
    dt = datetime.fromtimestamp(timestamp, tz=set_timezone)
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%f%z')

# Process message received by Kafka consumer  
def process_message(msg_value):
    try:
        # Parse the JSON message
        message_data = json.loads(msg_value)

        # Extract the event timestamp (in Unix timestamp format, seconds)
        upstream_event_timestamp = int(message_data.get('timestamp', 0))
        # Get Kafka straming latency
        kafka_consumer_timestamp, latency = calculate_latency(upstream_event_timestamp)
        
        # Adjust format to match with Spark log
        event_timestamp_iso = iso8601_format(upstream_event_timestamp)
        kafka_consumer_timestamp_iso = iso8601_format(kafka_consumer_timestamp)

        sensor_data = {
            'stream': message_data.get('stream', ''),
            'sensor_id': message_data.get('sensor_id', ''),
            'reading': message_data.get('reading', ''),
            'error': message_data.get('error', False),
            'upstream_event_timestamp': event_timestamp_iso,
            'kafka_consumer_timestamp': kafka_consumer_timestamp_iso,
            'latency': latency
        }

        # Print failed logs in console and write to CSV
        if sensor_data['error']:
            print(f"Failed Log: {sensor_data}")
            with open(output_csv, mode='a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow([sensor_data['stream'], sensor_data['sensor_id'], sensor_data['reading'],
                                 sensor_data['error'], sensor_data['upstream_event_timestamp'], sensor_data['kafka_consumer_timestamp'], 
                                 sensor_data['latency']])
    except Exception as e:
        print(f"Error processing message: {e}")

# Initiate messages polling from Kafka consumer
try:
    while True:
        msg = consumer.poll()  # Poll with a timeout of 1 second
        if msg is None:
            continue

        if msg.error():
            raise KafkaException(msg.error())
        else:
            message_value = msg.value().decode('utf-8')
            process_message(message_value)

finally:
    consumer.close()

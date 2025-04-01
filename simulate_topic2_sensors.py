from confluent_kafka import Producer
import json
import time
import random

stream2 = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to Kafka topic: {msg.topic()} at {int(time.time())}")

def generate_sensor_data():
    while True:      
        if_fault = bool(random.getrandbits(1))
        
        data = {
            'stream' : 2,
            'sensor_id': random.randint(1, 100),
            'reading': round(random.uniform(20.0, 30.0), 2),
            'error' : if_fault,
            'timestamp': int(time.time())
        }
        stream2.produce('sensor2-data', key=str(data['sensor_id']), value=json.dumps(data), callback=delivery_report)
        stream2.flush()
        time.sleep(2)

generate_sensor_data()
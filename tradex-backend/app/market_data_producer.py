from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

symbols = ['AAPL', 'GOOG', 'TSLA', 'MSFT']

while True:
    data = {
        'symbol': random.choice(symbols),
        'price': round(random.uniform(100, 1500), 2),
        'timestamp': time.time()
    }
    producer.send('market-data', value=data)
    print(f"Produced: {data}")
    time.sleep(1)

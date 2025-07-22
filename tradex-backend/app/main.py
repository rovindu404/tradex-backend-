from fastapi import FastAPI
from prometheus_client import start_http_server, Summary, Counter
import time
from threading import Thread
from kafka import KafkaConsumer
import json

app = FastAPI()

REQUEST_COUNT = Counter('request_count', 'App Request Count', ['method', 'endpoint'])

@app.middleware("http")
async def metrics_middleware(request, call_next):
    REQUEST_COUNT.labels(request.method, request.url.path).inc()
    response = await call_next(request)
    return response

@app.get("/ping")
def ping():
    return {"message": "pong"}

@app.get("/health")
def health():
    return {"status": "ok"}



def consume_market_data():
    consumer = KafkaConsumer(
        'market-data',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='tradex-consumer',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    for message in consumer:
        print(f"Consumed: {message.value}")

@app.on_event("startup")
def startup_event():
    Thread(target=consume_market_data, daemon=True).start()

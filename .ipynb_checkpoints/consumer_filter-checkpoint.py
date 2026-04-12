#file consumer_filter.py
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Nasłuchuję na duże transakcje (amount > 3000)...")

for message in consumer:
    transaction = message.value

    # sprawdzenie warunku
    if transaction["amount"] > 3000:
        print(
            f"ALERT: {transaction['tx_id']} | "
            f"{transaction['amount']} PLN | "
            f"{transaction['store']} | "
            f"{transaction['category']}"
        )
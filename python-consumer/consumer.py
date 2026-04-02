from kafka import KafkaConsumer
import json

# Настройка потребителя
consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',  # начать с самого первого сообщения
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: x.decode('utf-8')
)

print("Starting consumer...")
for message in consumer:
    print(f"Received: {message.value} | Partition: {message.partition} | Offset: {message.offset}")

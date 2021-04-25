from kafka import KafkaConsumer

consumer = KafkaConsumer(bootstrap_servers='localhost:9092', enable_auto_commit=False, metadata_max_age_ms=5000,
                         group_id='test-consumer-group')
consumer.subscribe(pattern='mytopic.*')

try:
    for msg in consumer:
        print(msg.value.decode('utf-8'))
        print(msg.key.decode('utf-8'))
        consumer.commit()
except Exception as e:
    print(e)
finally:
    consumer.close()

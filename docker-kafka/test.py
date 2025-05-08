from confluent_kafka import Producer
import time

conf = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print('❌ Delivery failed: {}'.format(err))
    else:
        print('✅ Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

while True:
    msg = f"Test message at {time.time()}"
    producer.produce('test-topic', msg.encode('utf-8'), callback=delivery_report)
    producer.poll(0)
    time.sleep(1)

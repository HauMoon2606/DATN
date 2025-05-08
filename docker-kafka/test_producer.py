from confluent_kafka import Producer
import json
import time

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def main():
    p = Producer({'bootstrap.servers':'localhost:9092'})
    with open('./test.json',encoding='utf-8') as f:
        data = json.load(f)
    for item in data:
        p.produce('test',json.dumps(item,ensure_ascii = False),callback =delivery_report )
        p.flush()
if __name__ == '__main__':
    main()

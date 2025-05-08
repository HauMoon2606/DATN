from confluent_kafka import Consumer
import json

def main():
    c = Consumer({'bootstrap.servers':'localhost:9092','group.id':'test_group','auto.offset.reset': 'earliest'})
    c.subscribe(['test'])
    consumed = []

    while True:
        msg = c.poll(1.0)

        if msg is None:
            print(len(consumed))
            continue
        if msg.error():
            print(f'Consumer error: {msg.error()}')
            continue
        
        msg_received = json.loads(msg.value().decode('utf-8'))
        consumed.append(msg_received)
        print(f'Received message: {msg_received}')
    c.close()

if __name__ =='__main__':
    main()
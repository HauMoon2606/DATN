from confluent_kafka import Consumer
import boto3
import json
import pandas as pd
from io import BytesIO

consumer_config = {
    'bootstrap.servers':'localhost:9092',
    'group.id':'minio_group',
    'auto.offset.reset':'earliest'
}

consumer = Consumer(consumer_config)
consumer.subscribe(['bds'])

minio_client = boto3.client(
    's3',
    endpoint_url = 'http://localhost:9002',
    aws_access_key_id = 'minioadmin',
    aws_secret_access_key='minioadmin'
)

bucket_name = 'bds'
file_key = 'bds.json'

try:
    minio_client.head_bucket(Bucket=bucket_name)
except:
    minio_client.create_bucket(Bucket=bucket_name)

records = []

print("Watting data from kafka")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Lỗi",msg.error())

        value = msg.value().decode('utf-8')
        record = json.loads(value)
        records.append(record)

finally:
    consumer.close()
    if records:
        # Ghi vào buffer dưới dạng JSON
        buffer = BytesIO()
        df = pd.DataFrame(records)
        buffer.write(df.to_json(orient='records', force_ascii=False).encode('utf-8'))
        buffer.seek(0)

        # Upload lên MinIO
        minio_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=buffer,
            ContentType='application/json'
        )

        print(f"Đã upload {len(records)} bản ghi lên MinIO tại {file_key}")
    else:
        print("Không có dữ liệu nào được đọc từ Kafka.")
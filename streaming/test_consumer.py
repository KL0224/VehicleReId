import os
import cv2
import yaml
import json
import base64
import numpy as np
from confluent_kafka import Consumer, KafkaException, KafkaError
from dotenv import load_dotenv

# Load config, env
load_dotenv()
with open("config/streaming/config_s02.yaml", 'r') as file:
    config = yaml.safe_load(file)

def run_consumer():
    """
    Nhận message từ kafka, giải mã và hiển thị khung hình để kiểm tra.
    """
    conf = {
        "bootstrap.servers": os.getenv('BOOTSTRAP_SERVERS'),
        'security.protocol': os.getenv('SECURITY_PROTOCOL'),
        'sasl.mechanisms': os.getenv('SASL_MECHANISM'),
        'sasl.username': os.getenv('SASL_USERNAME'),
        'sasl.password': os.getenv('SASL_PASSWORD'),
        "group.id": "video-test-consumer-group",
        "auto.offset.reset": "earliest",
        'fetch.message.max.bytes': 10485760
    }
    topic = config['kafka']['topic_test']['name']

    consumer = Consumer(conf)
    consumer.subscribe([topic])
    print(f"Đang lắng nghe topic {topic}...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            msg_value = json.loads(msg.value().decode("utf-8"))
            camera_id = msg_value['camera_id']
            frame_id = msg_value['frame_id']
            print(f"Nhận được frame {frame_id} từ camera {camera_id}")

            # Giải mã base64 và hiển thị ảnh
            frame_bytes = base64.b64decode(msg_value['frame_data'])
            frame_np = np.frombuffer(frame_bytes, dtype=np.uint8)
            frame = cv2.imdecode(frame_np, cv2.IMREAD_COLOR)

            if frame is not None:
                cv2.imshow(f"Camera: {camera_id}", frame)
                if cv2.waitKey(1) & 0xFF == ord('q'):
                    break
    except KeyboardInterrupt:
        print("Đã tạm dừng.")
    finally:
        consumer.close()
        cv2.destroyAllWindows()
        print("Consumer đã đóng!")

if __name__ == '__main__':
    run_consumer()
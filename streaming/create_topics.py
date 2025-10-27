import os
import yaml
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

# Tải biến môi truờng và cấu hình kafka
load_dotenv()
with open("config/streaming/config_s02.yaml", 'r') as file:
    config = yaml.safe_load(file)


def create_topic():
    """
    Kết nối tơ kafka server và tạo topics trong file config yaml
    """

    # Cấu hình kết nối tới kafka server
    admin_conf = {
        "bootstrap.servers": os.getenv('BOOTSTRAP_SERVERS'),
        'security.protocol': os.getenv('SECURITY_PROTOCOL'),
        'sasl.mechanisms': os.getenv('SASL_MECHANISM'),
        'sasl.username': os.getenv('SASL_USERNAME'),
        'sasl.password': os.getenv('SASL_PASSWORD'),
    }

    admin_client = AdminClient(admin_conf)

    # Lấy thông tin topics từ file config
    topics = config['kafka']['topics'].values()
    replication_factor = config['kafka']['topic_creation']['replication_factor']

    new_topics = [
        NewTopic(topic['name'], num_partitions=topic['partitions'], replication_factor=replication_factor)
        for topic in topics
    ]

    if not new_topics:
        print("Không có topic nào trong file config")
        return

    print(f"Đang yêu cầu tạo các topic: {list(new_topics)}...")
    fs = admin_client.create_topics(new_topics)

    # Đợi kết quả và in ra màn hình
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Đã tạo topic {topic} thành công")
        except Exception as e:
            if 'TOPIC_ALREADY_EXISTS' in str(e):
                print(f"Topic {topic} đã tồn tại.")
            else:
                print(f"Lỗi khi tạo topic {topic}: {e}")

if __name__ == '__main__':
    create_topic()
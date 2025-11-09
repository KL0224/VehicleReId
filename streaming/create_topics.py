import os
import yaml
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv
import sys


def load_kafka_config(config_path: str = "config/streaming/config_s02.yaml"):
    """Tải cấu hình từ file YAML với validation"""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Không tìm thấy file config: {config_path}")

    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    # Validate cấu trúc config
    if 'kafka' not in config or 'topics' not in config['kafka']:
        raise ValueError("File config thiếu phần 'kafka.topics'")

    return config


def create_topic(config_path: str = "config/streaming/config_s02.yaml"):
    """
    Kết nối tới Kafka server và tạo topics từ file config YAML
    """
    # Load environment variables
    load_dotenv()

    # Kiểm tra các biến môi trường cần thiết
    required_env_vars = [
        'BOOTSTRAP_SERVERS', 'SECURITY_PROTOCOL',
        'SASL_MECHANISM', 'SASL_USERNAME', 'SASL_PASSWORD'
    ]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        raise EnvironmentError(
            f"Thiếu các biến môi trường: {', '.join(missing_vars)}"
        )

    # Load config
    try:
        config = load_kafka_config(config_path)
    except Exception as e:
        print(f"❌ Lỗi khi load config: {e}")
        sys.exit(1)

    # Cấu hình kết nối tới Kafka server
    admin_conf = {
        "bootstrap.servers": os.getenv('BOOTSTRAP_SERVERS'),
        'security.protocol': os.getenv('SECURITY_PROTOCOL'),
        'sasl.mechanisms': os.getenv('SASL_MECHANISM'),
        'sasl.username': os.getenv('SASL_USERNAME'),
        'sasl.password': os.getenv('SASL_PASSWORD'),
        'socket.timeout.ms': 60000,  # Tăng timeout cho kết nối
    }

    try:
        admin_client = AdminClient(admin_conf)
        # Test kết nối
        metadata = admin_client.list_topics(timeout=10)
        print(f"✅ Kết nối Kafka thành công! Cluster có {len(metadata.topics)} topics.")
    except Exception as e:
        print(f"❌ Không thể kết nối tới Kafka server: {e}")
        sys.exit(1)

    # Lấy thông tin topics từ file config
    topics_config = config['kafka']['topics']
    replication_factor = config['kafka']['topic_creation']['replication_factor']

    # ✅ Cải thiện: sử dụng dict comprehension rõ ràng hơn
    new_topics = []
    for topic_key, topic_info in topics_config.items():
        topic_name = topic_info['name']
        partitions = topic_info['partitions']

        new_topics.append(
            NewTopic(
                topic=topic_name,
                num_partitions=partitions,
                replication_factor=replication_factor
            )
        )
        print(f"📝 Chuẩn bị tạo topic: {topic_name} ({partitions} partitions)")

    if not new_topics:
        print("⚠️ Không có topic nào trong file config")
        return

    # Tạo topics
    print(f"\n🚀 Đang tạo {len(new_topics)} topics...")
    fs = admin_client.create_topics(new_topics, request_timeout=30)

    # Đợi kết quả và in ra màn hình
    success_count = 0
    for topic, future in fs.items():
        try:
            future.result()  # Block until complete
            print(f"✅ Đã tạo topic '{topic}' thành công")
            success_count += 1
        except Exception as e:
            error_msg = str(e)
            if 'TOPIC_ALREADY_EXISTS' in error_msg:
                print(f"ℹ️  Topic '{topic}' đã tồn tại (bỏ qua)")
                success_count += 1
            else:
                print(f"❌ Lỗi khi tạo topic '{topic}': {e}")

    print(f"\n📊 Kết quả: {success_count}/{len(new_topics)} topics đã sẵn sàng")


if __name__ == '__main__':
    create_topic()
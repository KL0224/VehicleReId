import os
import cv2
import yaml
import json
import numpy as np
from confluent_kafka import Consumer, KafkaException, KafkaError
from dotenv import load_dotenv

load_dotenv()


def load_config(config_path="config/streaming/config_s02.yaml"):
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Không tìm thấy config: {config_path}")

    with open(config_path, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)


def run_consumer():
    """
    Nhận message từ Kafka, giải mã và hiển thị khung hình.
    """
    config = load_config()

    conf = {
        "bootstrap.servers": os.getenv('BOOTSTRAP_SERVERS'),
        'security.protocol': os.getenv('SECURITY_PROTOCOL'),
        'sasl.mechanisms': os.getenv('SASL_MECHANISM'),
        'sasl.username': os.getenv('SASL_USERNAME'),
        'sasl.password': os.getenv('SASL_PASSWORD'),
        "group.id": "video-test-consumer-group",
        "auto.offset.reset": "earliest",
        'fetch.message.max.bytes': 10485760,
        'session.timeout.ms': 45000,
        'max.poll.interval.ms': 300000,
    }

    topic = config['kafka']['topics']['raw_frames']['name']

    consumer = Consumer(conf)
    consumer.subscribe([topic])

    print("=" * 60)
    print(f"🎬 KAFKA VIDEO CONSUMER")
    print(f"📡 Topic: {topic}")
    print(f"🔑 Group ID: {conf['group.id']}")
    print("=" * 60)
    print("Đang lắng nghe... (nhấn 'q' để thoát)\n")

    frame_count = 0
    camera_windows = {}  # Track các window đã mở

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"ℹ️  Đã đọc hết partition {msg.partition()}")
                    continue
                else:
                    raise KafkaException(msg.error())

            # Parse metadata từ headers
            headers = msg.headers() or []
            header_dict = {k: v for k, v in headers}

            meta_json = header_dict.get("meta")
            if meta_json is None:
                print("⚠️  Message thiếu metadata header!")
                continue

            try:
                metadata = json.loads(meta_json.decode("utf-8"))
            except json.JSONDecodeError as e:
                print(f"❌ Lỗi parse metadata: {e}")
                continue

            camera_id = metadata.get("camera_id", "unknown")
            frame_id = metadata.get("frame_id", -1)
            timestamp = metadata.get("timestamp", "N/A")
            frame_timestamp = metadata.get("frame_timestamp", 0.0)

            # ✅ Cải thiện: chỉ in log mỗi 100 frames
            if frame_count % 100 == 0:
                print(f"📥 Nhận frame #{frame_count} | "
                      f"Camera: {camera_id} | "
                      f"Frame ID: {frame_id} | "
                      f"Time: {frame_timestamp:.2f}s")

            # Giải mã ảnh
            frame_bytes = msg.value()
            frame_np = np.frombuffer(frame_bytes, dtype=np.uint8)
            frame = cv2.imdecode(frame_np, cv2.IMREAD_COLOR)

            if frame is None:
                print(f"⚠️  Không thể decode frame {frame_id} từ camera {camera_id}")
                continue

            # ✅ Vẽ overlay thông tin
            info_text = f"{camera_id} | Frame: {frame_id} | {frame_timestamp:.2f}s"
            cv2.putText(
                frame, info_text, (10, 30),
                cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 0), 2
            )

            # Hiển thị
            window_name = f"Camera: {camera_id}"
            cv2.imshow(window_name, frame)
            camera_windows[camera_id] = window_name

            frame_count += 1

            # Kiểm tra phím thoát
            if cv2.waitKey(1) & 0xFF == ord('q'):
                print("\n🛑 Người dùng yêu cầu dừng")
                break

    except KeyboardInterrupt:
        print("\n⏹️  Đã dừng (Ctrl+C)")

    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()

    finally:
        print(f"\n📊 Tổng số frames đã nhận: {frame_count:,}")
        print("Đang đóng consumer...")
        consumer.close()
        cv2.destroyAllWindows()
        print("✅ Consumer đã đóng!")


if __name__ == '__main__':
    run_consumer()
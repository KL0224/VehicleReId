import os
import json
import cv2
import yaml
import time
import threading
import signal
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from datetime import datetime
from dotenv import load_dotenv

# --- Load environment ---
load_dotenv()

# --- Biến toàn cục để kiểm soát việc dừng ---
stop_event = threading.Event()

def signal_handler(sig, frame):
    print("\n🛑 Nhận tín hiệu Ctrl+C — đang dừng tất cả stream...")
    stop_event.set()
signal.signal(signal.SIGINT, signal_handler)

# --- Hàm load config ---
def load_config(config_path='config/streaming/config_s02.yaml'):
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

# --- Hàm kiểm tra topic trước khi stream video lên Kafka cloud ---  
def check_topic_exists(topic_name: str):
    """Kiểm tra xem topic có tồn tại trên Kafka (Redpanda Cloud) không"""
    admin_config = {
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS"),
        "security.protocol": os.getenv("SECURITY_PROTOCOL", "SASL_SSL"),
        "sasl.mechanisms": os.getenv("SASL_MECHANISM", "SCRAM-SHA-256"),
        "sasl.username": os.getenv("SASL_USERNAME"),
        "sasl.password": os.getenv("SASL_PASSWORD"),
    }

    admin_client = AdminClient(admin_config)
    try:
        metadata = admin_client.list_topics(timeout=10)
        if metadata is None:
            print("❌ Không thể lấy metadata từ Kafka (metadata=None)")
            return False

        topics = metadata.topics.keys()
        return topic_name in topics
    except Exception as e:
        print(f"❌ Lỗi khi kiểm tra topic Kafka: {e}")
        return False

# --- Tạo Kafka Producer ---
def create_producer():
    conf = {
        "bootstrap.servers": os.getenv('BOOTSTRAP_SERVERS'),
        'security.protocol': os.getenv('SECURITY_PROTOCOL', 'SASL_SSL'),
        'sasl.mechanisms': os.getenv('SASL_MECHANISM', 'SCRAM-SHA-256'),
        'sasl.username': os.getenv('SASL_USERNAME'),
        'sasl.password': os.getenv('SASL_PASSWORD'),
        "linger.ms": 20,
        "compression.type": 'snappy',
        "message.max.bytes": 10485760,
        "request.timeout.ms": 60000,
        "retries": 5,
        "retry.backoff.ms": 1000,
        "message.timeout.ms": 600000
    }

    print(f"🚀 Kết nối Kafka broker: {conf['bootstrap.servers']}")
    return Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Lỗi khi gửi message (key={msg.key().decode('utf-8')}): {err}")

# --- Hàm stream video ---
def stream_video(video_path, camera_id, producer, topic, frame_rate_limit):
    try:
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            print(f"❌ Không thể mở video: {video_path}")
            return 0

        print(f"▶️ Bắt đầu stream từ camera {camera_id}...")
        frame_id = 0
        start_stream_time = time.time()  # ⏱ Bắt đầu tính FPS trung bình

        while cap.isOpened() and not stop_event.is_set():
            ret, frame = cap.read()
            if not ret or stop_event.is_set():
                break

            frame = cv2.resize(frame, (1440, 810))
            ok, buffer = cv2.imencode('.jpg', frame, [int(cv2.IMWRITE_JPEG_QUALITY), 80])
            if not ok:
                continue

            frame_bytes = buffer.tobytes()
            metadata = {
                "camera_id": camera_id,
                "timestamp": datetime.now().isoformat(),
                "frame_id": frame_id,
                "frame_timestamp": frame_id / frame_rate_limit,
            }

            try:
                start_time = time.time()

                producer.produce(
                    topic=topic,
                    value=frame_bytes,
                    key=camera_id.encode('utf-8'),
                    headers=[('meta', json.dumps(metadata).encode('utf-8'))],
                    callback=delivery_report
                )

                elapsed_ms = (time.time() - start_time) * 1000
                size_kb = len(frame_bytes) / 1024

                print(f"[{camera_id}] Gửi frame {frame_id:04d} "
                      f"({size_kb:.1f} KB, {elapsed_ms:.1f} ms)")

                # 📈 In FPS trung bình mỗi 100 frame
                if frame_id > 0 and frame_id % 100 == 0:
                    avg_fps = frame_id / (time.time() - start_stream_time)
                    print(f"📊 [{camera_id}] Tốc độ trung bình: {avg_fps:.2f} FPS")

            except BufferError:
                print(f"⚠️ Buffer đầy — đợi gửi tiếp {camera_id}...")
                producer.flush(5)
                if stop_event.is_set():
                    break

            producer.poll(0)
            frame_id += 1
            time.sleep(1 / frame_rate_limit)

    except Exception as e:
        print(f"❌ Lỗi trong luồng {camera_id}: {e}")

    finally:
        cap.release()
        print(f"⏹ Dừng stream cho camera {camera_id}")
        return frame_id

# --- Main ---
if __name__ == '__main__':
    config = load_config()
    producer_config = config['producer']
    kafka_config = config['kafka']
    producer = create_producer()
    raw_frames_topic = kafka_config['topics']['raw_frames']['name']

    # 🔍 Kiểm tra topic tồn tại
    print(f"🔍 Kiểm tra topic '{raw_frames_topic}' trên Kafka...")
    if not check_topic_exists(raw_frames_topic):
        print(f"❌ Topic '{raw_frames_topic}' không tồn tại! Dừng chương trình.")
        producer.flush(2)
        exit(1)
    print(f"✅ Topic '{raw_frames_topic}' tồn tại. Bắt đầu stream...\n")

    # Ước tính tổng số khung hình
    total_frames_to_send = 0
    video_paths = {}
    print("📊 Ước tính tổng số khung hình cần gửi...")
    for cam_id in producer_config['cameras_to_stream']:
        video_path = os.path.join(producer_config['video_source_dir'], f'{cam_id}.avi')
        if not os.path.exists(video_path):
            print(f"❌ Không tìm thấy video: {video_path}")
            continue

        video_paths[cam_id] = video_path
        cap = cv2.VideoCapture(video_path)
        if cap.isOpened():
            frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            total_frames_to_send += frame_count
            print(f"  - {cam_id}.avi: {frame_count} frames")
            cap.release()
        else:
            print(f"⚠️ Không thể mở video {video_path}")

    print(f"==> Tổng số message dự kiến gửi: {total_frames_to_send}\n")

    threads = []
    sent_counts = {}

    for cam_id, video_path in video_paths.items():
        sent_counts[cam_id] = 0
        thread = threading.Thread(
            target=lambda c=cam_id, p=video_path: sent_counts.update({
                c: stream_video(p, c, producer, raw_frames_topic, producer_config['frame_rate_limit'])
            }),
            daemon=True  # 👈 giúp Ctrl+C dừng ngay
        )
        threads.append(thread)
        thread.start()

    # --- Main loop kiểm tra Ctrl+C ---
    try:
        while any(t.is_alive() for t in threads):
            time.sleep(0.5)
            if stop_event.is_set():
                break
    except KeyboardInterrupt:
        print("\n🛑 Nhận Ctrl+C, dừng chương trình...")
        stop_event.set()

    print("\n⏳ Đang gửi nốt các message còn lại...")
    producer.flush(10)
    print("✅ Đã gửi hết các message.")

    # Báo cáo kết quả
    total_frames_sent = sum(sent_counts.values())
    print("\n--- KẾT QUẢ ---")
    print(f"Tổng số message dự kiến: {total_frames_to_send}")
    print(f"Tổng số message thực tế đã gửi: {total_frames_sent}")

    if total_frames_to_send == total_frames_sent:
        print("🎉 THÀNH CÔNG: Đã gửi tất cả các khung hình!")
    else:
        print(f"⚠️ Thiếu {total_frames_to_send - total_frames_sent} message.")
        for cam_id, count in sent_counts.items():
            print(f"  - Camera {cam_id}: {count} frames")

    print("\n👋 Đã thoát an toàn.")

import os
import json
import cv2
import base64
import yaml
import time
import threading
from confluent_kafka import Producer
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def load_config(config_path='config/streaming/config_s02.yaml'):
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def create_producer():
    """
    Khởi tạo Kafka producer với cấu hình từ file môi trường
    """
    conf = {
        "bootstrap.servers": os.getenv('BOOTSTRAP_SERVERS'),
        'security.protocol': os.getenv('SECURITY_PROTOCOL'),
        'sasl.mechanisms': os.getenv('SASL_MECHANISM'),
        'sasl.username': os.getenv('SASL_USERNAME'),
        'sasl.password': os.getenv('SASL_PASSWORD'),
        "linger.ms": 20, # Gửi message theo batch, đợi 20ms
        "compression.type": 'snappy', # Nén message
        "message.max.bytes": 10485760, # Cho phép gửi message lên 10MB
        "request.timeout.ms": 60000, # Tăng thời gian phản hồi cho mỗi request
        "retries": 5, # Tự động gửi lại message 5 lần nếu thất bại
        "retry.backoff.ms": 1000, # Đợi 1 giây trước khi thử lại
        "message.timeout.ms": 600000  # Tăng tổng thời gian tối đa cho một message lên 10 phút
    }

    return Producer(conf)

def delivery_report(err, msg):
    """
    Callback khi message được gửi hoặc thất bại!
    """
    if err is not None:
        print(f"Lỗi khi gửi message (key={msg.key().decode('utf-8')}): {err}")


def stream_video(video_path, camera_id, producer, topic, frame_rate_limit):
    """
    Đọc một file video, tách khung hình và gửi tới kafka!
    Trả về số lượng khung hình đã gửi.
    """
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        print(f"Lỗi không thể mở video từ file {video_path}")
        return 0  # Trả về 0 nếu không mở được video

    print(f"Bắt đầu stream từ camera {camera_id}...")
    frame_id = 0
    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            # Kết thúc video, thoát vòng lặp
            break

        # Encoder khung hình sang base64
        ok, buffer = cv2.imencode('.jpg', frame)
        if not ok:
            continue
        frame_bytes = buffer.tobytes()

        # Tạo message JSON
        metadata = {
            "camera_id": camera_id,
            "timestamp": datetime.now().isoformat(),
            "frame_id": frame_id,
            "frame_timestamp": frame_id / frame_rate_limit,
        }

        try:
            # Gửi messgae tới kafka
            producer.produce(
                topic=topic,
                value=frame_bytes,
                key=camera_id.encode('utf-8'),
                headers=[('meta', json.dumps(metadata).encode('utf-8'))],
                callback=delivery_report
            )
            print(f"Đã gửi frame {frame_id}...")
        except BufferError:
            # Nếu buffer đầy, đợi và thử lại sau.
            print(f"Buffer của producer bị đầy cho camera {camera_id}. Đang đợi...")
            producer.flush(5)  # Cố gắng gửi message trong 5 giây
            # Thử lại một lần nữa
            try:
                producer.produce(
                    topic=topic,
                    value=frame_bytes,
                    key=camera_id.encode('utf-8'),
                    headers=[('meta', json.dumps(metadata).encode('utf-8'))],
                    callback=delivery_report
                )
            except BufferError:
                print(f"Lỗi nghiêm trọng: Không thể gửi message cho camera {camera_id} ngay cả sau khi đợi, bỏ qua khung hình này")
                # Tại đây, message này sẽ bị mất. Chúng ta dừng stream cho camera này.
                break

        producer.poll(0)  # Trigger callback

        frame_id += 1
        time.sleep(1 / frame_rate_limit)

    cap.release()
    print(f"Kết thúc stream cho camera {camera_id}...")
    return frame_id  # Trả về tổng số khung hình đã xử lý

if __name__ == '__main__':
    config = load_config()

    producer_config = config['producer']
    kafka_config = config['kafka']

    producer = create_producer()
    raw_frames_topic = kafka_config['topics']['raw_frames']['name']

    # 1. Ước tính tổng số message sẽ được gửi
    total_frames_to_send = 0
    video_paths = {}
    print("Đang ước tính tổng số khung hình...")
    for cam_id in producer_config['cameras_to_stream']:
        video_path = os.path.join(producer_config['video_source_dir'], f'{cam_id}.avi')
        if not os.path.exists(video_path):
            print(f"Không tìm thấy đường dẫn video {video_path}.")
            continue

        video_paths[cam_id] = video_path
        cap = cv2.VideoCapture(video_path)
        if cap.isOpened():
            frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            total_frames_to_send += frame_count
            print(f"  - Video '{cam_id}.avi': {frame_count} khung hình.")
            cap.release()
        else:
            print(f"Lỗi không thể mở video {video_path} để đếm khung hình.")

    print(f"==> Tổng số message dự kiến gửi: {total_frames_to_send}\n")

    # 2. Chuẩn bị để đếm số message thực tế
    threads = []
    sent_counts = {}  # Dictionary để lưu số lượng message đã gửi của mỗi luồng

    for cam_id, video_path in video_paths.items():
        # Khởi tạo bộ đếm cho camera này
        sent_counts[cam_id] = 0

        # Sửa đổi target của thread để truyền vào sent_counts
        thread = threading.Thread(
            target=lambda: sent_counts.update({cam_id: stream_video(
                video_path, cam_id, producer, raw_frames_topic, producer_config['frame_rate_limit']
            )})
        )
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print("\nĐang đợi gửi hết các message còn lại...")
    producer.flush()  # Đảm bảo tất cả message được gửi đi
    print("Tất cả các luồng stream đã hoàn tất.")

    # 3. So sánh kết quả
    total_frames_sent = sum(sent_counts.values())
    print("\n--- KẾT QUẢ SO SÁNH ---")
    print(f"Tổng số message dự kiến: {total_frames_to_send}")
    print(f"Tổng số message thực tế đã gửi: {total_frames_sent}")

    if total_frames_to_send == total_frames_sent:
        print("==> THÀNH CÔNG: Đã gửi tất cả các khung hình!")
    else:
        print(f"==> CẢNH BÁO: Bị thiếu {total_frames_to_send - total_frames_sent} message.")
        print("Chi tiết mỗi luồng:")
        for cam_id, count in sent_counts.items():
            print(f"  - Camera {cam_id}: đã gửi {count} message.")
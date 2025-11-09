import os
import json
import cv2
import time
import threading
from confluent_kafka import Producer
from datetime import datetime
from dotenv import load_dotenv
import yaml

load_dotenv()


def load_config(config_path='config/streaming/config_s02.yaml'):
    """Load cấu hình với validation"""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Không tìm thấy file config: {config_path}")

    with open(config_path, 'r', encoding='utf-8') as f:
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
        "linger.ms": 20,
        "compression.type": 'snappy',
        "message.max.bytes": 10485760,
        "request.timeout.ms": 60000,
        "retries": 5,
        "retry.backoff.ms": 1000,
        "message.timeout.ms": 600000,
        # ✅ Thêm: tối ưu batch processing
        "batch.size": 1000000,  # 1MB batch
        "queue.buffering.max.messages": 100000,
    }
    return Producer(conf)


def delivery_report(err, msg):
    """Callback khi message được gửi hoặc thất bại"""
    if err is not None:
        camera_id = msg.key().decode('utf-8') if msg.key() else 'unknown'
        print(f"❌ Lỗi gửi message (camera={camera_id}): {err}")


def stream_video(video_path, camera_id, producer, topic, frame_rate_limit,
                 start_time_offset=0.0):
    """
    Đọc file video, tách khung hình và gửi tới Kafka.

    Args:
        video_path: Đường dẫn file video
        camera_id: ID camera
        producer: Kafka Producer instance
        topic: Tên topic Kafka
        frame_rate_limit: FPS mong muốn
        start_time_offset: Offset thời gian bắt đầu (giây, dùng cho đồng bộ multi-cam)

    Returns:
        Số lượng khung hình đã gửi thành công
    """
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        print(f"❌ Lỗi: Không thể mở video '{video_path}'")
        return 0

    # Lấy thông tin video
    original_fps = cap.get(cv2.CAP_PROP_FPS)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    print(f"📹 Camera {camera_id}: {total_frames} frames, FPS gốc={original_fps:.2f}")

    frame_id = 0
    sent_count = 0
    failed_count = 0

    # ✅ Timestamp chuẩn hóa: epoch của video (1970-01-01 + offset)
    # Để đồng bộ multi-camera, có thể thêm start_time_offset
    video_start_epoch = datetime(1970, 1, 1).timestamp() + start_time_offset

    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break

        # Resize frame
        frame = cv2.resize(frame, (1440, 810))

        # Encode frame thành JPEG binary
        ok, buffer = cv2.imencode('.jpg', frame, [int(cv2.IMWRITE_JPEG_QUALITY), 80])
        if not ok:
            print(f"⚠️  Camera {camera_id}: Không encode được frame {frame_id}")
            frame_id += 1
            continue

        frame_bytes = buffer.tobytes()

        # ✅ Tính timestamp theo chuẩn ISO8601 (frame_timestamp = thời điểm tương đối trong video)
        frame_timestamp_seconds = frame_id / frame_rate_limit
        absolute_timestamp = video_start_epoch + frame_timestamp_seconds

        # Tạo metadata theo chuẩn schemas.py
        metadata = {
            "camera_id": camera_id,
            "frame_id": frame_id,
            "timestamp": datetime.utcfromtimestamp(absolute_timestamp).isoformat(timespec="milliseconds") + "Z",
            "frame_timestamp": frame_timestamp_seconds,  # Thời gian tương đối (giây)
            "width": 1440,
            "height": 810,
        }

        try:
            # Gửi message tới Kafka
            producer.produce(
                topic=topic,
                value=frame_bytes,
                key=camera_id.encode('utf-8'),
                headers=[
                    ('content-type', b'image/jpeg'),
                    ('meta', json.dumps(metadata).encode('utf-8'))
                ],
                callback=delivery_report
            )
            sent_count += 1

            # ✅ Cải thiện: chỉ in log mỗi 100 frames
            if frame_id % 100 == 0:
                print(f"📤 Camera {camera_id}: đã gửi {sent_count}/{frame_id + 1} frames "
                      f"({len(frame_bytes) // 1024}KB)")

        except BufferError:
            # Buffer đầy, đợi và thử lại
            print(f"⚠️  Camera {camera_id}: Buffer đầy tại frame {frame_id}, đang flush...")
            producer.flush(10)  # Đợi tối đa 10s

            # Thử lại lần cuối
            try:
                producer.produce(
                    topic=topic,
                    value=frame_bytes,
                    key=camera_id.encode('utf-8'),
                    headers=[
                        ('content-type', b'image/jpeg'),
                        ('meta', json.dumps(metadata).encode('utf-8'))
                    ],
                    callback=delivery_report
                )
                sent_count += 1
            except Exception as e:
                print(f"❌ Camera {camera_id}: Không thể gửi frame {frame_id} sau khi retry: {e}")
                failed_count += 1
                # Tiếp tục thay vì break để không mất toàn bộ stream

        except Exception as e:
            print(f"❌ Camera {camera_id}: Lỗi không xác định tại frame {frame_id}: {e}")
            failed_count += 1

        # Trigger callbacks
        producer.poll(0)

        frame_id += 1

        # Đồng bộ FPS
        time.sleep(1.0 / frame_rate_limit)

    cap.release()
    print(f"✅ Camera {camera_id}: Hoàn thành - Gửi {sent_count}/{frame_id} frames, "
          f"thất bại {failed_count}")

    return sent_count


def main():
    """Main function với xử lý lỗi đầy đủ"""
    try:
        config = load_config()
    except Exception as e:
        print(f"❌ Lỗi load config: {e}")
        return

    producer_config = config['producer']
    kafka_config = config['kafka']

    producer = create_producer()
    raw_frames_topic = kafka_config['topics']['raw_frames']['name']

    # 1. Ước tính tổng số frames
    total_frames_to_send = 0
    video_paths = {}

    print("=" * 60)
    print("📊 ƯỚC TÍNH SỐ LƯỢNG FRAMES")
    print("=" * 60)

    for cam_id in producer_config['cameras_to_stream']:
        video_path = os.path.join(producer_config['video_source_dir'], f'{cam_id}.avi')

        if not os.path.exists(video_path):
            print(f"⚠️  Không tìm thấy video: {video_path}")
            continue

        video_paths[cam_id] = video_path
        cap = cv2.VideoCapture(video_path)

        if cap.isOpened():
            frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            fps = cap.get(cv2.CAP_PROP_FPS)
            duration = frame_count / fps if fps > 0 else 0

            total_frames_to_send += frame_count
            print(f"  📹 {cam_id}.avi: {frame_count} frames "
                  f"(FPS={fps:.2f}, thời lượng={duration:.2f}s)")
            cap.release()
        else:
            print(f"❌ Không thể mở video: {video_path}")

    print(f"\n🎯 Tổng số frames dự kiến: {total_frames_to_send:,}")
    print("=" * 60)

    if not video_paths:
        print("❌ Không có video nào để stream!")
        return

    # 2. Khởi chạy multi-threading
    threads = []
    sent_counts = {}

    print(f"\n🚀 BẮT ĐẦU STREAMING TỪ {len(video_paths)} CAMERA(S)")
    print("=" * 60)

    for cam_id, video_path in video_paths.items():
        # ✅ SỬA: Sử dụng default argument để capture biến đúng
        def stream_wrapper(camera_id=cam_id, path=video_path):
            count = stream_video(
                path,
                camera_id,
                producer,
                raw_frames_topic,
                producer_config['frame_rate_limit']
            )
            sent_counts[camera_id] = count

        thread = threading.Thread(target=stream_wrapper, name=f"Thread-{cam_id}")
        threads.append(thread)
        thread.start()

    # Đợi tất cả threads hoàn thành
    for thread in threads:
        thread.join()

    # 3. Flush tất cả messages còn lại
    print("\n⏳ Đang flush messages còn lại trong buffer...")
    remaining = producer.flush(30)  # Đợi tối đa 30s

    if remaining > 0:
        print(f"⚠️  Còn {remaining} messages chưa được gửi!")
    else:
        print("✅ Đã gửi tất cả messages")

    # 4. Tổng kết
    total_frames_sent = sum(sent_counts.values())

    print("\n" + "=" * 60)
    print("📈 KẾT QUẢ STREAMING")
    print("=" * 60)
    print(f"Dự kiến:   {total_frames_to_send:,} frames")
    print(f"Đã gửi:    {total_frames_sent:,} frames")
    print(f"Tỷ lệ:     {total_frames_sent / total_frames_to_send * 100:.2f}%")

    print("\n📊 Chi tiết từng camera:")
    for cam_id in sorted(sent_counts.keys()):
        count = sent_counts[cam_id]
        print(f"  • {cam_id}: {count:,} frames")

    if total_frames_to_send == total_frames_sent:
        print("\n✅ THÀNH CÔNG: Đã stream tất cả frames!")
    else:
        diff = total_frames_to_send - total_frames_sent
        print(f"\n⚠️  CẢNH BÁO: Thiếu {diff:,} frames ({diff / total_frames_to_send * 100:.2f}%)")

    print("=" * 60)


if __name__ == '__main__':
    main()
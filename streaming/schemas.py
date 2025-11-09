from __future__ import annotations
import json
from typing import Any, Dict, List, Iterable, Tuple, Union, Optional
from datetime import datetime, timedelta

# SCHEMA_VERSION
SCHEMA_VERSION = "1.0"

# TOPIC NAMES
TOPIC_RAW_FRAMES = "raw_frames"
TOPIC_DETECTIONS = "detections"
TOPIC_TRACKS = "tracks"
TOPIC_MTMCT = "mtmc_tracks"

# Content type markers (hữu ích cho headers/observability)
CONTENT_TYPE_JSON = "application/json"

# Một số hàm tiện ích
def _iso_ts(ts: Union[str, int, float, datetime, None]) -> str:
    """
       Chuẩn hóa timestamp thành chuỗi ISO8601.
       Hỗ trợ cả timestamp của frame video (float giây từ đầu video).
       """
    if ts is None:
        # Nếu không có ts -> lấy thời gian hiện tại (UTC)
        return datetime.utcnow().isoformat(timespec="milliseconds") + "Z"

    if isinstance(ts, str):
        # Nếu đã là chuỗi ISO rồi -> giữ nguyên
        return ts

    if isinstance(ts, datetime):
        # Nếu là datetime -> ép về UTC và format ISO
        if ts.tzinfo is None:
            return ts.isoformat(timespec="milliseconds") + "Z"
        return ts.isoformat()

    if isinstance(ts, (int, float)):
        # Trường hợp mô phỏng video: frame_timestamp = frame_id / fps
        # Lấy mốc "epoch" (1970-01-01) + số giây tương đối đó
        base = datetime(1970, 1, 1)
        return (base + timedelta(seconds=float(ts))).isoformat(timespec="milliseconds") + "Z"

    raise TypeError(f"Không hỗ trợ loại timestamp: {type(ts)}")

def _ensure_bbox(b: Iterable[Union[int, float]]) -> List[float]:
    """Kiểm tra định dạng bbox xem có phải là [x1, y1, x2, y2] hay [x, y, w, h] hay không"""
    arr = list(map(float, b))
    if len(arr) != 4:
        raise ValueError("bbox phải là [x1, y1, x2, y2] hoặc [x, y, w, h]")
    return arr

def _ensure_float(x: Any, name: str) -> float:
    """Đảm bảo giá trị là float"""
    try:
        return float(x)
    except Exception as e:
        raise ValueError(f"{name} phải là số thực: {e}")

def _ensure_int(x: Any, name: str) -> int:
    """Đảm bảo giá trị là int"""
    try:
        return int(x)
    except Exception as e:
        raise ValueError(f"{name} phải là số nguyên: {e}")

def _non_empty_str(x: Any, name: str) -> str:
    """Đảm bảo giá trị là chuỗi không rỗng"""
    if not isinstance(x, str) or not x:
        raise ValueError(f"{name} phải là chuỗi không rỗng")
    return x

def _bytes_key(x: str) -> bytes:
    """Chuyển key (str) sang dạng byte"""
    return x.encode("utf-8")

# VALIDATE SCHEMA FOR RAW_FRAMES
def validate_raw_frame_metadata(meta: Dict[str, Any]) -> None:
    """
    Validate metadata của raw frame.

    Metadata structure:
    {
        "type": "raw_frame",
        "camera_id": "c001",
        "frame_id": 123,
        "timestamp": "2024-01-15T10:30:45.123Z",
        "frame_timestamp": 4.1,
        "width": 1440,
        "height": 810,
        "encoding": "jpeg"
    }
    """
    if meta.get("type") != "raw_frame":
        raise ValueError("type phải là 'raw_frame'")

    _non_empty_str(meta.get("camera_id"), "camera_id")
    _ensure_int(meta.get("frame_id"), "frame_id")
    _ensure_int(meta.get("width"), "width")
    _ensure_int(meta.get("height"), "height")
    _ensure_float(meta.get("frame_timestamp"), "frame_timestamp")

    # Validate encoding
    encoding = meta.get("encoding", "jpeg")
    if encoding not in ["jpeg", "png", "raw"]:
        raise ValueError(f"encoding không hợp lệ: {encoding}")

# SCHEMA for Detections
def make_detections_msg(
        camera_id: str,
        frame_id: int,
        ts: Union[str, int, float, datetime, None],
        width: int,
        height: int,
        detections: List[Dict[str, Any]],
        *,
        schema_version: str = SCHEMA_VERSION,
        extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Xây dựng 1 detection payload.
    detections có dạng:
    { "bbox":[x1,y1,x2,y2], "conf":float, "cls_id":int, "cls_name":optional str, "attr":optional dict }
    """
    msg = {
        "type": "detections",
        "schema_version": schema_version,
        "camera_id": _non_empty_str(camera_id, "camera_id"),
        "frame_id": _ensure_int(frame_id, "frame_id"),
        "timestamp": _iso_ts(ts),
        "image_size": {"w": _ensure_int(width, "width"), "h": _ensure_int(height, "height")},
        "detections": [],
    }

    for d in detections:
        bbox = _ensure_bbox(d.get("bbox", []))
        conf = _ensure_float(d.get("conf", 0.0), "conf")
        cls_id = _ensure_int(d.get("cls_id", -1), "cls_id")
        det = {"bbox": bbox, "conf": conf, "cls_id": cls_id}

        if "cls_name" in d:
            det["cls_name"] = str(d["cls_name"])
        if "attr" in d and isinstance(d["attr"], dict):
            det["attr"] = d["attr"]
        msg["detections"].append(det)

    if extra:
        msg["extra"] = extra
    return msg

def validate_detections_msg(msg: Dict[str, Any]) -> None:
    """Hàm đánh giá message detections"""
    if msg.get("type") != "detections":
        raise ValueError("Không phải là detections")
    _non_empty_str(msg.get("camera_id"), "camera_id")
    _ensure_int(msg.get("frame_id"), "frame_id")
    if not isinstance(msg.get("image_size"), dict):
        raise ValueError("Image_size phải là dict")
    _ensure_int(msg["image_size"].get("w"), "image_size.w")
    _ensure_int(msg["image_size"].get("h"), "image_size.h")
    dets = msg.get("detections", [])
    if not isinstance(dets, list):
        raise ValueError("detections phải là list")
    for d in dets:
        _ensure_bbox(d.get("bbox", []))
        _ensure_float(d.get("conf", 0.0), "conf")
        _ensure_int(d.get("cls_id", -1), "cls_id")

# SCHEMA MESSAGE Tracks (MOT)
def make_tracks_msg(
        camera_id: str,
        frame_id: int,
        ts: Union[str, int, float, datetime, None],
        tracks: List[Dict[str, Any]],
        *,
        schema_version: str = SCHEMA_VERSION,
        extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    xây dựng 1 track payload.
    tracks item shape:
      { "track_id":int, "bbox":[x1,y1,x2,y2], "conf":float, "cls_id":int,
        "feature":optional List[float], "speed":optional float, "direction":optional float, "attr":optional dict }
    """
    msg = {
        "type": "tracks",
        "schema_version": schema_version,
        "camera_id": _non_empty_str(camera_id, "camera_id"),
        "frame_id": _ensure_int(frame_id, "frame_id"),
        "timestamp": _iso_ts(ts),
        "tracks": [],
    }

    for t in tracks:
        track_id = _ensure_int(t.get("track_id", -1), "track_id")
        bbox = _ensure_bbox(t.get("bbox", []))
        conf = _ensure_float(t.get("conf", 0.0), "conf")
        cls_id = _ensure_int(t.get("cls_id", -1), "cls_id")
        item: Dict[str, Any] = {"track_id": track_id, "bbox": bbox, "conf": conf, "cls_id": cls_id}

        if "feature" in t and isinstance(t["feature"], (list, tuple)):
            item["feature"] = [float(x) for x in t["feature"]]
        if "speed" in t:
            item["speed"] = _ensure_float(t["speed"], "speed")
        if "direction" in t:
            item["direction"] = _ensure_float(t["direction"], "direction")
        msg["tracks"].append(item)

    if extra:
        msg["extra"] = extra

    return msg


def validate_track_msg(msg: Dict[str, Any]) -> None:
    """Validate message tracks"""
    if msg.get("type") != "tracks":
        raise ValueError("invalid type for tracks")

    _non_empty_str(msg.get("camera_id"), "camera_id")
    _ensure_int(msg.get("frame_id"), "frame_id")

    trs = msg.get("tracks", [])
    if not isinstance(trs, list):
        raise ValueError("tracks must be a list")

    for idx, t in enumerate(trs):
        # Validate bắt buộc
        _ensure_int(t.get("track_id", -1), f"tracks[{idx}].track_id")
        _ensure_bbox(t.get("bbox", []))
        _ensure_float(t.get("conf", 0.0), f"tracks[{idx}].conf")
        _ensure_int(t.get("cls_id", -1), f"tracks[{idx}].cls_id")

        if "feature" in t:
            feature = t["feature"]
            if not isinstance(feature, list):
                raise ValueError(f"tracks[{idx}].feature phải là list")
            if len(feature) == 0:
                raise ValueError(f"tracks[{idx}].feature không được rỗng")

        if "speed" in t:
            _ensure_float(t["speed"], f"tracks[{idx}].speed")

        if "direction" in t:
            direction = _ensure_float(t["direction"], f"tracks[{idx}].direction")
            if not (0.0 <= direction <= 360.0):
                raise ValueError(
                    f"tracks[{idx}].direction phải trong [0, 360], nhận được: {direction}"
                )

# SCHEMA MESSGAE MTMC

def make_mtmc_msg(
    global_id: Union[int, str],
    camera_trajectories: List[Dict[str, Any]],
    *,
    ts: Union[str, int, float, datetime, None] = None,
    matching_confidence: Optional[float] = None,
    attributes: Optional[Dict[str, Any]] = None,
    schema_version: str = SCHEMA_VERSION,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Xây dựng 1 message mtmc (Đây là output cuối cùng của pipeline).
    """
    msg = {
        "type": "mtmc",
        "schema_version": schema_version,
        "timestamp": _iso_ts(ts),
        "global_id": str(global_id),
        "trajectories": [],
    }
    # Validate và thêm trajectories
    for traj in camera_trajectories:
        camera_id = _non_empty_str(traj.get("camera_id"), "camera_id")
        local_track_id = _ensure_int(traj.get("local_track_id"), "local_track_id")
        timestamps = traj.get("timestamps", [])
        boxes = traj.get("boxes", [])

        if len(timestamps) != len(boxes):
            raise ValueError(
                f"Camera {camera_id}: timestamps ({len(timestamps)}) "
                f"và boxes ({len(boxes)}) phải cùng số lượng"
            )

        traj_item = {
            "camera_id": camera_id,
            "local_track_id": local_track_id,
            "timestamps": [_iso_ts(t) for t in timestamps],
            "boxes": [_ensure_bbox(b) for b in boxes],
        }

        if "feature" in traj and isinstance(traj["feature"], (list, tuple)):
            traj_item["feature"] = [float(x) for x in traj["feature"]]

        msg["trajectories"].append(traj_item)

    # Optional fields
    if matching_confidence is not None:
        msg["matching_confidence"] = _ensure_float(
            matching_confidence, "matching_confidence"
        )

    if attributes:
        msg["attributes"] = attributes

    if extra:
        msg["extra"] = extra

    return msg

def validate_mtmc_msg(msg: Dict[str, Any]) -> None:
    if msg.get("type") != "mtmc":
        raise ValueError("Không phải mtmc")
    _non_empty_str(msg.get("global_id"), "global_id")
    trajectories = msg.get("trajectories", [])
    if not isinstance(trajectories, list):
        raise ValueError("trajectories phải là list")

    if len(trajectories) == 0:
        raise ValueError("trajectories không được rỗng")

    # 4. Validate từng trajectory
    for idx, traj in enumerate(trajectories):
        if not isinstance(traj, dict):
            raise ValueError(f"trajectory[{idx}] phải là dict")
        _non_empty_str(traj.get("camera_id"), f"trajectory[{idx}].camera_id")
        _ensure_int(traj.get("local_track_id"), f"trajectory[{idx}].local_track_id")
        timestamps = traj.get("timestamps", [])
        if not isinstance(timestamps, list):
            raise ValueError(f"trajectory[{idx}].timestamps phải là list")
        boxes = traj.get("boxes", [])
        if not isinstance(boxes, list):
            raise ValueError(f"trajectory[{idx}].boxes phải là list")
        if len(timestamps) != len(boxes):
            raise ValueError(
                f"trajectory[{idx}]: timestamps ({len(timestamps)}) và "
                f"boxes ({len(boxes)}) phải cùng số lượng"
            )
        if len(timestamps) == 0:
            raise ValueError(f"trajectory[{idx}]: timestamps/boxes không được rỗng")
        for b_idx, box in enumerate(boxes):
            try:
                _ensure_bbox(box)
            except ValueError as e:
                raise ValueError(
                    f"trajectory[{idx}].boxes[{b_idx}] không hợp lệ: {e}"
                )
        if "feature" in traj:
            feature = traj["feature"]
            if not isinstance(feature, list):
                raise ValueError(f"trajectory[{idx}].feature phải là list")
            if len(feature) == 0:
                raise ValueError(f"trajectory[{idx}].feature không được rỗng")
    if "matching_confidence" in msg:
        conf = _ensure_float(msg["matching_confidence"], "matching_confidence")
        if not (0.0 <= conf <= 1.0):
            raise ValueError(
                f"matching_confidence phải trong khoảng [0, 1], nhận được: {conf}"
            )
    if "attributes" in msg:
        if not isinstance(msg["attributes"], dict):
            raise ValueError("attributes phải là dict")

# JSON + Kafka helpers

def dumps(msg: Dict[str, Any]) -> str:
    return json.dumps(msg, separators=(",", ":"), ensure_ascii=False)

def loads(s: str) -> Dict[str, Any]:
    return json.loads(s)

def to_kafka_record(
    msg: Dict[str, Any],
    *,
    key: Optional[str] = None,
) -> Tuple[bytes, str]:
    """
    Produce (key_bytes, json_value) cho Kafka sink.
    Default keys:
      - detections/tracks: camera_id
      - mtmc: global_id
    """
    t = msg.get("type")
    if key is None:
        if t in ("detections", "tracks"):
            key = msg.get("camera_id")
        elif t == "mtmc":
            key = msg.get("global_id")
        else:
            key = "unknown"
    return _bytes_key(str(key)), dumps(msg)


def parse_and_validate(value_str: str) -> Dict[str, Any]:
    """
    Parse JSON và validate theo message type.

    Raises:
        ValueError: Nếu message không hợp lệ
    """
    obj = loads(value_str)
    msg_type = obj.get("type")

    if msg_type == "raw_frame":
        validate_raw_frame_metadata(obj)
    elif msg_type == "detections":
        validate_detections_msg(obj)
    elif msg_type == "tracks":
        validate_track_msg(obj)
    elif msg_type == "mtmc":
        validate_mtmc_msg(obj)
    else:
        raise ValueError(f"Unknown message type: {msg_type}")

    return obj

def build_headers(meta: Optional[Dict[str, Any]] = None) -> List[Tuple[str, bytes]]:
    """
    Standardize headers (content-type + optional meta).
    Spark Kafka source will surface headers as array<struct<key,value>>.
    """
    headers: List[Tuple[str, bytes]] = [("content-type", CONTENT_TYPE_JSON.encode("utf-8"))]
    if meta:
        headers.append(("meta", json.dumps(meta).encode("utf-8")))
    return headers


# Test validate_mtmc_msg()
if __name__ == "__main__":
    # ✅ Valid message
    valid_msg = {
        "type": "mtmc",
        "global_id": "v001",
        "timestamp": "2024-01-15T10:30:45.123Z",
        "matching_confidence": 0.92,
        "trajectories": [
            {
                "camera_id": "c001",
                "local_track_id": 5,
                "timestamps": ["2024-01-15T10:30:45.123Z"],
                "boxes": [[100, 200, 150, 300]],
                "feature": [0.1] * 512
            }
        ]
    }

    try:
        validate_mtmc_msg(valid_msg)
        print("✅ Valid message passed")
    except ValueError as e:
        print(f"❌ Validation failed: {e}")

    # ❌ Invalid message (thiếu trajectories)
    invalid_msg = {
        "type": "mtmc",
        "global_id": "v001",
        "trajectories": []  # Rỗng → lỗi
    }

    try:
        validate_mtmc_msg(invalid_msg)
        print("❌ Should have failed!")
    except ValueError as e:
        print(f"✅ Correctly caught error: {e}")





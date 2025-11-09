from __future__ import annotations
import os
from pathlib import Path
import threading
import torch
import logging

logger = logging.getLogger(__name__)

# Global singleton state --> Load model 1 lần duy nhất
_MODEL = None
_LOCK = threading.Lock()

def _select_device() -> torch.device:
    """Cho phép ghi đè thiết bị thogno qua envm ngược lại dùng cuda nêếu có sẵn"""
    dev = os.getenv("DETECT_DEVICE", "").strip().lower()
    if dev in ("cpu", "cuda", "cuda:0"):
        return torch.device(dev if dev != "cuda" else "cuda:0")
    return torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

def get_detector() -> torch.nn.Module:
    """
    Trả về 1 yolov5 model được load từ local repo và đưa lên device
    Model được tạo chỉ 1 lần cho mỗi tiến trình.
    """
    global _MODEL
    if _MODEL is not None:
        return _MODEL

    with _LOCK:
        if _MODEL is not None:
            return _MODEL

        here = Path(__file__).resolve().parent
        yolo_dir = here / "yolov5"
        weights = yolo_dir / "yolov5x6.pt"

        if not yolo_dir.exists():
            raise FileNotFoundError(f"Không tìm thấy thư mục yolov5 tại {yolo_dir}.")
        if not weights.exists():
            raise FileNotFoundError(f"Không tìm thấy weights yolov5 tại {weights}.")

        device = _select_device()
        logger.info(f"🔧 Loading YOLOv5 model on device: {device}")

        model = torch.hub.load(str(yolo_dir), "custom", path=str(weights), source="local")
        model.to(device).eval()

        # Config from env
        conf = float(os.getenv("DETECT_CONF", "0.5"))
        iou = float(os.getenv("DETECT_IOU", "0.45"))
        model.conf = conf
        model.iou = iou
        logger.info(f"📊 Model config: conf={conf}, iou={iou}")

        # Half precision
        if device.type == "cuda":
            try:
                model.half()
                logger.info("⚡ Enabled FP16 inference")
            except Exception as e:
                logger.warning(f"⚠️ Could not enable FP16: {e}")

        _MODEL = model
        logger.info("✅ YOLOv5 model loaded successfully")

    return _MODEL



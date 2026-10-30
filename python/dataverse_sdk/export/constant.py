from enum import Enum

GROUNDTRUTH = "groundtruth"
GROUND_TRUTH_ANNOTATION_NAME = "ground_truths"

MAX_CONCURRENT_DOWNLOADS = 100
BATCH_SIZE = 50

VIDEO_SUPPORTED_FORMAT = {".mp4", ".avi", ".mov", ".mpeg", ".flv", ".wmv"}


class ExportFormat(str, Enum):
    COCO = "coco"
    VISIONAI = "visionai"
    TRAINING_FORMAT = "training_format"
    YOLO = "yolo"
    VLM = "vlm"
    LLAVA = "llava"

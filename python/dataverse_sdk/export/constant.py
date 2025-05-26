from enum import Enum

GROUNDTRUTH = "groundtruth"
GROUND_TRUTH_ANNOTATION_NAME = "ground_truths"

MAX_CONCURRENT_DOWNLOADS = 100
BATCH_SIZE = 50


class ExportFormat(str, Enum):
    COCO = "coco"
    VISIONAI = "visionai"
    TRAINING_FORMAT = "training_format"
    YOLO = "yolo"
    VLM = "vlm"

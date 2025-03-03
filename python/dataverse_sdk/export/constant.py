from enum import Enum

GROUND_TRUTH_ANNOTATION_NAME = "ground_truths"


class ExportFormat(str, Enum):
    COCO = "coco"
    VISIONAI = "visionai"
    TRAINING_FORMAT = "training_format"
    YOLO = "yolo"
    VLM = "vlm"

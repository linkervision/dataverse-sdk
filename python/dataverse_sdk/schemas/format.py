from enum import Enum


class AnnotationFormat(str, Enum):
    VISION_AI = "vision_ai"
    COCO = "coco"
    YOLO = "yolo"
    VLM = "vlm"

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from ..constants import BaseEnumMeta


class AttributeType(str, Enum, metaclass=BaseEnumMeta):
    BOOLEAN = "boolean"
    OPTION = "option"
    NUMBER = "number"
    TEXT = "text"


class OntologyImageType(str, Enum, metaclass=BaseEnumMeta):
    _2D_BOUNDING_BOX = "2d_bounding_box"
    SEMANTIC_SEGMENTATION = "semantic_segmentation"
    INSTANCE_SEGMENTATION = "instance_segmentation"
    CLASSIFICATION = "classification"
    POINT = "point"
    POLYGON = "polygon"
    POLYLINE = "polyline"
    VQA = "vqa"


class OntologyPcdType(str, Enum, metaclass=BaseEnumMeta):
    CUBOID = "cuboid"


class SensorType(str, Enum, metaclass=BaseEnumMeta):
    CAMERA = "camera"
    LIDAR = "lidar"


class AnnotationFormat(str, Enum, metaclass=BaseEnumMeta):
    VISION_AI = "vision_ai"
    COCO = "coco"
    BDDP = "bddp"
    IMAGE = "image"
    KITTI = "kitti"
    YOLO = "yolo"
    VIDEO = "video"
    VLM = "vlm"


class DatasetType(str, Enum, metaclass=BaseEnumMeta):
    ANNOTATED_DATA = "annotated_data"
    RAW_DATA = "raw_data"


class DatasetStatus(str, Enum, metaclass=BaseEnumMeta):
    PROCESSING = "processing"
    FAIL = "fail"
    READY = "ready"


class DataSource(str, Enum, metaclass=BaseEnumMeta):
    AWS = "aws"
    LOCAL = "local"
    SDK = "sdk"
    EXISTING_DATASETS = "existing_datasets"
    EXISTING_DATASLICES = "existing_dataslices"
    DATA_GENERATION = "data_generation"
    PRE_IMPORT = "pre_import"


class ConvertModelFileType(str, Enum, metaclass=BaseEnumMeta):
    """Which stored artifact of a convert record to download."""

    TRITON = "triton"
    MODEL = "model"
    RAW_ONNX = "raw_onnx"
    CALIB_CACHE = "calib_cache"


# Last-resort local filename per artifact, used only when the caller gives no
# save_path AND the response carries no usable Content-Disposition filename. The
# server's own name is authoritative: it reflects the convert format and precision
# (e.g. "ptq.engine"), which a static table cannot. Treat these as a floor to fall
# back on, not as a description of what the file contains.
CONVERT_MODEL_FILE_DEFAULT_SAVE_PATHS: dict[ConvertModelFileType, str] = {
    ConvertModelFileType.TRITON: "./triton.zip",
    ConvertModelFileType.MODEL: "./converted_model",
    ConvertModelFileType.RAW_ONNX: "./raw.onnx",
    ConvertModelFileType.CALIB_CACHE: "./trt_int8_calib.cache",
}


class ModelStructure(str, Enum, metaclass=BaseEnumMeta):
    """Architecture to build uploaded custom model weights into."""

    YOLOV9_C = "yolov9-c"
    YOLOV9_E = "yolov9-e"
    YOLOV9_S = "yolov9-s"
    DFINE_N = "dfine-n"
    DFINE_S = "dfine-s"
    DFINE_M = "dfine-m"
    DFINE_L = "dfine-l"
    DFINE_X = "dfine-x"


class ConvertFormat(str, Enum, metaclass=BaseEnumMeta):
    """Target format a model is converted into."""

    ONNX = "onnx"
    TRT = "trt"


class ConvertPrecision(str, Enum, metaclass=BaseEnumMeta):
    """Numeric precision the converted model runs at."""

    FP32 = "fp32"
    FP16 = "fp16"
    INT8 = "int8"


class QuantizationMethod(str, Enum, metaclass=BaseEnumMeta):
    """How an int8 model is quantized. Only meaningful with ConvertPrecision.INT8."""

    PTQ = "ptq"
    QAT_TRAIN = "qat_train"
    QAT_DISTILL = "qat_distill"


class DataSliceStatus(str, Enum, metaclass=BaseEnumMeta):
    """DataSlice lifecycle: CREATING, then READY or CREATING_FAIL; *_UPDATING are transient."""

    CREATING = "creating"
    CREATING_FAIL = "creating_fail"
    READY = "ready"
    ANNOTATION_UPDATING = "annotation_updating"
    IQA_UPDATING = "iqa_updating"
    TAGGING_UPDATING = "tagging_updating"
    DELETING = "deleting"


class MLModelStatus(str, Enum, metaclass=BaseEnumMeta):
    """ML model lifecycle: PROCESSING, then READY or DELETING."""

    PROCESSING = "processing"
    READY = "ready"
    DELETING = "deleting"


class ConvertRecordStatus(str, Enum, metaclass=BaseEnumMeta):
    """Convert record lifecycle: PROCESSING, then READY, FAILED or DELETING."""

    PROCESSING = "processing"
    READY = "ready"
    FAILED = "failed"
    DELETING = "deleting"


CONVERT_RESOLUTIONS: frozenset[tuple[int, int]] = frozenset(
    {
        (640, 480),
        (640, 640),
        (1024, 576),
        (1024, 768),
        (1024, 1024),
    }
)

DFINE_FORMAT_BY_PRECISION: dict[str, str] = {
    ConvertPrecision.FP32.value: ConvertFormat.ONNX.value,
    ConvertPrecision.FP16.value: ConvertFormat.TRT.value,
    ConvertPrecision.INT8.value: ConvertFormat.TRT.value,
}

DFINE_MODEL_STRUCTURES: frozenset[str] = frozenset(
    {
        ModelStructure.DFINE_N.value,
        ModelStructure.DFINE_S.value,
        ModelStructure.DFINE_M.value,
        ModelStructure.DFINE_L.value,
        ModelStructure.DFINE_X.value,
    }
)

DFINE_QUANTIZATION_METHODS: frozenset[str] = frozenset({QuantizationMethod.PTQ.value})

# Only D-FINE converts at fp32; every other structure is limited to these two.
NMS_CONVERT_PRECISIONS: frozenset[str] = frozenset(
    {ConvertPrecision.FP16.value, ConvertPrecision.INT8.value}
)


@dataclass
class SensorCounts:
    camera: int = 0
    lidar: int = 0


@dataclass
class ProjectCreateDatasetConfig:
    annotation_format: AnnotationFormat
    dataset_type: DatasetType
    sensor_counts: SensorCounts
    is_sequential: bool
    image_type: Optional[OntologyImageType] = None
    pcd_type: Optional[OntologyPcdType] = None
    has_attribute: bool = False

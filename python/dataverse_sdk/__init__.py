import logging
import re

from . import connections
from .client import DataverseClient
from .constants import DataverseHost
from .schemas.client import (
    Attribute,
    AttributeOption,
    ConvertRecord,
    Dataset,
    Ontology,
    OntologyClass,
    Project,
    ProjectTag,
    QuestionClass,
    Sensor,
)
from .schemas.common import (
    AnnotationFormat,
    AttributeType,
    ConvertFormat,
    ConvertModelFileType,
    ConvertPrecision,
    ConvertRecordStatus,
    DataSliceStatus,
    DatasetStatus,
    DatasetType,
    DataSource,
    MLModelStatus,
    ModelStructure,
    OntologyImageType,
    OntologyPcdType,
    QuantizationMethod,
    SensorType,
)

__all__ = [
    "DataverseClient",
    "DataverseHost",
    "AttributeType",
    "OntologyImageType",
    "OntologyPcdType",
    "SensorType",
    "Attribute",
    "AttributeOption",
    "Ontology",
    "OntologyClass",
    "Project",
    "Dataset",
    "Sensor",
    "ProjectTag",
    "connections",
    "AnnotationFormat",
    "DatasetType",
    "DatasetStatus",
    "DataSource",
    "QuestionClass",
    "ConvertModelFileType",
    "ConvertFormat",
    "ConvertPrecision",
    "ConvertRecordStatus",
    "ConvertRecord",
    "QuantizationMethod",
    "DataSliceStatus",
    "MLModelStatus",
    "ModelStructure",
]


class _SensitiveDataFilter(logging.Filter):
    """Filter to redact sensitive credentials from logs"""

    SENSITIVE_PATTERNS = [
        (r"AWSAccessKeyId=[^&\s]*", "AWSAccessKeyId=***"),
        (r"Signature=[^&\s]*", "Signature=***"),
        (r"Expires=\d+", "Expires=***"),
    ]

    def filter(self, record):
        # Override getMessage to process the final formatted message
        original_getMessage = record.getMessage

        def patched_getMessage():
            msg = original_getMessage()
            for pattern, replacement in self.SENSITIVE_PATTERNS:
                msg = re.sub(pattern, replacement, msg)
            return msg

        record.getMessage = patched_getMessage
        return True


logging.getLogger("httpx").addFilter(_SensitiveDataFilter())

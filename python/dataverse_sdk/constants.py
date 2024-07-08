from enum import Enum, EnumMeta
from typing import Any, Optional


class BaseEnumMeta(EnumMeta):
    _value_set: Optional[set[Any]] = None

    def __contains__(cls, item):
        if cls._value_set is None:
            cls._value_set: set[Any] = {v.value for v in cls.__members__.values()}
        return item in cls._value_set


class DataverseHost(str, Enum, metaclass=BaseEnumMeta):
    DEV = "https://dev.visionai.linkervision.ai/dataverse/curation"
    DEV2 = "https://dev2.visionai.linkervision.ai/dataverse/curation"
    DEV3 = "https://dev3.visionai.linkervision.ai/dataverse/curation"
    STAGING = "https://staging.visionai.linkervision.ai/dataverse/curation"
    DEMO = "https://demo.dataverse.linkervision.ai"
    PRODUCTION = "https://dataverse.linkervision.ai"
    LOCAL = "http://localhost:8000"

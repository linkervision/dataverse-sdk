import json
import os
from collections.abc import AsyncGenerator
from typing import Callable

from visionai_data_format.utils.common import ANNOT_PATH

from .base import ExportAnnotationBase
from .constant import ExportFormat
from .exporter import Exporter
from .utils import convert_to_bytes, download_url_file_async

VLM_ANNOTATION_FILE = "vlm_annotation.json"


@Exporter.register(format=ExportFormat.VLM)
class ExportVQA(ExportAnnotationBase):
    async def producer(
        self,
        class_names: list[str],
        sequence_frame_map: dict[int, dict[int, list[int]]],
        question_id_map: dict,
        datarow_generator_func: Callable[[list], AsyncGenerator[dict]],
        annotation_name: str,
        *_,
        **kwargs,
    ) -> AsyncGenerator[bytes, str]:
        datarows = []

        for frame_datarow_map in sequence_frame_map.values():
            for datarow_ids in frame_datarow_map.values():
                async for datarow in datarow_generator_func(datarow_ids):
                    datarows.append(datarow)

                    img_bytes: bytes = await download_url_file_async(datarow["url"])
                    original_file_name = os.path.basename(datarow["original_url"])
                    yield (
                        img_bytes,
                        os.path.join("images", original_file_name),
                    )

        annot_bytes: bytes = convert_to_bytes(
            convert_annotation(
                datarows=datarows,
                annotation_name=annotation_name,
                question_id_map=question_id_map,
            )
        )
        yield annot_bytes, os.path.join(ANNOT_PATH, VLM_ANNOTATION_FILE)


def convert_annotation(
    datarows: list[dict], annotation_name: str, question_id_map: dict
) -> str:
    annotation_list = []
    image_id_start = 0
    for datarow in datarows:
        url = datarow["url"]
        file_extension = os.path.splitext(url)[-1]
        image_id = f"{image_id_start:012d}"
        new_image_name = f"{image_id}{file_extension}"

        vlm_annotation = datarow["vlm_items"]["data"]
        vlm_annotation["id"] = image_id
        vlm_annotation["image"] = new_image_name
        new_conversations = []
        for conversation in vlm_annotation["conversations"]:
            if conversation["answer"].get(annotation_name) is None:
                continue
            conversation["answer"] = {
                annotation_name: conversation["answer"][annotation_name]
            }
            conversation["question"] = question_id_map[conversation["question_id"]]
            new_conversations.append(conversation)
        vlm_annotation["conversations"] = new_conversations

        annotation_list.append(vlm_annotation)
        image_id_start += 1

    return json.dumps(annotation_list)

import os
from collections.abc import AsyncGenerator
from typing import Callable

from visionai_data_format.converters.vai_to_yolo import VAItoYOLO

from .base import ExportAnnotationBase
from .constant import GROUND_TRUTH_ANNOTATION_NAME, ExportFormat
from .exporter import Exporter
from .utils import convert_to_bytes, download_url_file_async


@Exporter.register(format=ExportFormat.YOLO)
class ExportYolo(ExportAnnotationBase):
    async def producer(
        self,
        class_names: list[str],
        sequence_frame_map: dict[int, dict[int, list[int]]],
        datarow_generator_func: Callable[[list], AsyncGenerator[dict]],
        annotation_name: str,
        *_,
        **kwargs,
    ) -> AsyncGenerator[bytes, str]:
        current_file_count = 0
        category_map = {class_name: i for i, class_name in enumerate(class_names)}
        for frame_datarow_map in sequence_frame_map.values():
            for datarow_ids in frame_datarow_map.values():
                async for datarow in datarow_generator_func(datarow_ids):
                    img_bytes: bytes = await download_url_file_async(datarow["url"])
                    original_file_name = os.path.basename(datarow["original_url"])
                    yield (
                        img_bytes,
                        os.path.join("images", original_file_name),
                    )

                    annot_bytes: bytes = convert_to_bytes(
                        convert_annotation(
                            datarow=datarow,
                            category_map=category_map,
                            annotation_name=annotation_name,
                        )
                    )
                    yield (
                        annot_bytes,
                        os.path.join("labels", f"{current_file_count:012d}.txt"),
                    )

                    current_file_count += 1

        yield convert_to_bytes("\n".join(class_names)), "classes.txt"


def convert_annotation(
    datarow: dict,
    category_map: dict,
    annotation_name: str,
) -> dict:
    """
    vai to yolo convert

    Returns
    -------
    str
    """

    if annotation_name == GROUND_TRUTH_ANNOTATION_NAME:
        visionai_dict: dict = datarow["items"].get(annotation_name, {})
    else:
        visionai_dict: dict = (
            datarow["items"].get("predictions", {}).get(annotation_name, {})
        )

    (category_map, image_labels_map, _, _) = VAItoYOLO.convert_single_visionai_to_yolo(
        dest_img_folder="",
        visionai_dict={"visionai": visionai_dict},
        copy_sensor_data=False,
        source_data_root="",
        uri_root="",
        camera_sensor_name=datarow["sensor_name"],
        image_id_start=0,
        category_map=category_map,
        n_frame=-1,
        img_extension=".jpg",
        img_width=datarow["image_width"],
        img_height=datarow["image_height"],
    )
    # only get the labels dict of first item since the data visionai contain only single image data
    labels: list = next(iter(image_labels_map.values()))
    return "\n".join(labels)

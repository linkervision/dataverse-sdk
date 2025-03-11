import os
from collections.abc import AsyncGenerator
from typing import Callable

from visionai_data_format.converters.vai_to_coco import VAItoCOCO
from visionai_data_format.schemas.coco_schema import COCO, Category
from visionai_data_format.utils.common import (
    ANNOT_PATH,
    COCO_IMAGE_PATH,
    COCO_LABEL_FILE,
)

from .base import ExportAnnotationBase
from .constant import GROUND_TRUTH_ANNOTATION_NAME, ExportFormat
from .exporter import Exporter
from .utils import convert_to_bytes, download_url_file_async


@Exporter.register(format=ExportFormat.COCO)
class ExportCoco(ExportAnnotationBase):
    async def producer(
        self,
        class_names: list[str],
        sequence_frame_map: dict[int, dict[int, list[int]]],
        datarow_generator_func: Callable[[list], AsyncGenerator[dict]],
        target_folder: str,
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
                        os.path.join(
                            COCO_IMAGE_PATH,
                            original_file_name,
                        ),
                    )

        annot_bytes: bytes = convert_to_bytes(
            convert_annotation(
                datarows=datarows,
                class_names=class_names,
                target_folder=target_folder,
                annotation_name=annotation_name,
            )
        )
        yield annot_bytes, os.path.join(ANNOT_PATH, COCO_LABEL_FILE)


def convert_annotation(
    datarows: list[dict],
    class_names: list[str],
    target_folder: str,
    annotation_name: str,
) -> dict:
    image_id_start = 0
    anno_id_start = 0
    images = []
    annotations = []
    category_idx_map = {category: idx for idx, category in enumerate(class_names)}
    for datarow in datarows:
        camera_sensor_name = datarow["sensor_name"]
        url = datarow["url"]
        file_extension = os.path.splitext(url)[-1]

        if annotation_name == GROUND_TRUTH_ANNOTATION_NAME:
            target_visionai: dict = datarow["items"].get(annotation_name, {})
        else:
            target_visionai: dict = (
                datarow["items"].get("predictions", {}).get(annotation_name, {})
            )

        (
            category_idx_map,
            image_update,
            anno_update,
            image_id_start,
            anno_id_start,
            _,
        ) = VAItoCOCO.convert_single_visionai_to_coco(
            dest_img_folder=os.path.join(target_folder, COCO_IMAGE_PATH),
            visionai_dict={"visionai": target_visionai},
            copy_sensor_data=False,
            source_data_root="",
            uri_root=target_folder,
            camera_sensor_name=camera_sensor_name,
            image_id_start=image_id_start,
            anno_id_start=anno_id_start,
            category_map=category_idx_map,
            n_frame=-1,
            img_extension=file_extension,
            img_width=datarow["image_width"],
            img_height=datarow["image_height"],
        )
        images.extend(image_update)
        annotations.extend(anno_update)
    # generate category objects
    categories = [
        Category(
            id=class_id,
            name=class_name,
        )
        for class_name, class_id in category_idx_map.items()
    ]
    coco = COCO(categories=categories, images=images, annotations=annotations).dict()
    return coco

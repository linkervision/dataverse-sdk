import asyncio
import os
from collections.abc import AsyncGenerator
from typing import Callable

import aiohttp
from tqdm import tqdm
from visionai_data_format.converters.vai_to_coco import VAItoCOCO
from visionai_data_format.schemas.coco_schema import COCO, Category
from visionai_data_format.utils.common import (
    ANNOT_PATH,
    COCO_IMAGE_PATH,
    COCO_LABEL_FILE,
)

from .base import ExportAnnotationBase
from .constant import (
    BATCH_SIZE,
    GROUND_TRUTH_ANNOTATION_NAME,
    GROUNDTRUTH,
    MAX_CONCURRENT_DOWNLOADS,
    ExportFormat,
)
from .exporter import Exporter
from .utils import convert_to_bytes, gen_empty_vai


@Exporter.register(format=ExportFormat.COCO)
class ExportCoco(ExportAnnotationBase):
    async def download_batch(
        self,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        batch_datarows: list[dict],
    ) -> list[tuple[bytes, str]]:
        tasks = []

        for datarow in batch_datarows:
            url = datarow["url"]
            file_path = os.path.join(COCO_IMAGE_PATH, datarow["unique_file_name"])

            async def download_single(url, file_path, max_retries=5, initial_delay=1):
                async with semaphore:
                    delay = initial_delay
                    for attempt in range(max_retries):
                        try:
                            async with session.get(url) as response:
                                response.raise_for_status()
                                img_bytes = await response.read()
                                return img_bytes, file_path
                        except Exception as e:
                            if attempt == max_retries - 1:
                                print(
                                    f"Error downloading {url} after {max_retries} attempts: {e}"
                                )
                                return None
                            print(
                                f"Attempt {attempt + 1} failed for {url}: {e}. Retrying in {delay} seconds..."
                            )
                            await asyncio.sleep(delay)
                            delay *= 2

            tasks.append(download_single(url, file_path))

        results = await asyncio.gather(*tasks)
        return [r for r in results if r is not None]

    async def producer(
        self,
        class_names: list[str],
        sequence_frame_map: dict[int, dict[int, list[int]]],
        datarow_generator_func: Callable[[list], AsyncGenerator[dict]],
        target_folder: str,
        annotation_name: str,
        *_,
        **kwargs,
    ) -> AsyncGenerator[tuple[bytes, str], None]:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
        async with aiohttp.ClientSession() as session:
            current_batch = []
            datarows = []  # Keep track of all datarows for annotation
            datarow_id_list = []
            total_datarows = sum(len(v) for v in sequence_frame_map.values())
            existing_files = set()

            with tqdm(
                total=total_datarows, desc="Downloading images", unit="file"
            ) as progress_bar:
                for frame_datarow_map in sequence_frame_map.values():
                    for datarow_ids in frame_datarow_map.values():
                        datarow_id_list.extend(datarow_ids)
                for start_idx in range(0, len(datarow_id_list), BATCH_SIZE):
                    async for datarow in datarow_generator_func(
                        datarow_id_list[start_idx : start_idx + BATCH_SIZE]
                    ):
                        original_file_name = os.path.basename(datarow["original_url"])
                        unique_file_name = Exporter.get_unique_filename(
                            self, original_file_name, existing_files
                        )
                        existing_files.add(unique_file_name)
                        datarow["unique_file_name"] = unique_file_name
                        current_batch.append(datarow)
                        datarows.append(datarow)

                        if len(current_batch) >= BATCH_SIZE:
                            results = await self.download_batch(
                                session, semaphore, current_batch
                            )
                            for result in results:
                                if result:
                                    yield result
                                    progress_bar.update(1)
                            current_batch = []

                if current_batch:
                    results = await self.download_batch(
                        session, semaphore, current_batch
                    )
                    for result in results:
                        if result:
                            yield result
                            progress_bar.update(1)

        annot_bytes = convert_to_bytes(
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
        if annotation_name == GROUNDTRUTH:
            target_visionai: dict = datarow["items"].get(
                GROUND_TRUTH_ANNOTATION_NAME, {}
            )
        else:
            target_visionai: dict = (
                datarow["items"].get("predictions", {}).get(annotation_name, {})
            )

        if not target_visionai:
            target_visionai = gen_empty_vai(datarow=datarow, sequence_folder_url="")

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
        image_update[0].file_name = datarow["unique_file_name"]
        image_update[
            0
        ].coco_url = (
            f"{image_update[0].coco_url.rsplit('/',1)[0]}/{datarow['unique_file_name']}"
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

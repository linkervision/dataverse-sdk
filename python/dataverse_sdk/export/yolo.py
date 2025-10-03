import asyncio
import os
from collections.abc import AsyncGenerator
from typing import Callable

import aiohttp
from tqdm import tqdm
from visionai_data_format.converters.vai_to_yolo import VAItoYOLO

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


@Exporter.register(format=ExportFormat.YOLO)
class ExportYolo(ExportAnnotationBase):
    async def download_batch(
        self,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        batch_datarows: list[dict],
        category_map: dict,
        annotation_name: str,
    ) -> list[tuple[bytes, str]]:
        tasks = []
        results = []
        for datarow in batch_datarows:
            url = datarow["url"]
            file_name_without_format = datarow["unique_file_name"].split(".")[0]
            anno_name = f"{file_name_without_format}.txt"
            file_path = os.path.join("images", datarow["unique_file_name"])
            anno_path = os.path.join("labels", anno_name)

            annot_bytes: bytes = convert_to_bytes(
                convert_annotation(
                    datarow=datarow,
                    category_map=category_map,
                    annotation_name=annotation_name,
                )
            )
            results.append((annot_bytes, anno_path))

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

        results.extend(await asyncio.gather(*tasks))

        return [r for r in results if r is not None]

    async def producer(
        self,
        class_names: list[str],
        sequence_frame_map: dict[int, dict[int, list[int]]],
        datarow_generator_func: Callable[[list], AsyncGenerator[dict]],
        annotation_name: str,
        *_,
        **kwargs,
    ) -> AsyncGenerator[bytes, str]:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
        async with aiohttp.ClientSession() as session:
            current_batch = []
            datarow_id_list = []
            total_datarows = sum(len(v) for v in sequence_frame_map.values())
            category_map = {class_name: i for i, class_name in enumerate(class_names)}
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
                        if len(current_batch) >= BATCH_SIZE:
                            results = await self.download_batch(
                                session,
                                semaphore,
                                current_batch,
                                category_map,
                                annotation_name,
                            )
                            for result in results:
                                if result:
                                    yield result
                                    progress_bar.update(1)
                            current_batch = []

                if current_batch:
                    results = await self.download_batch(
                        session, semaphore, current_batch, category_map, annotation_name
                    )
                    for result in results:
                        if result:
                            yield result
                            progress_bar.update(1)

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
    if annotation_name == GROUNDTRUTH:
        visionai_dict: dict = datarow["items"].get(GROUND_TRUTH_ANNOTATION_NAME, {})
    else:
        visionai_dict: dict = (
            datarow["items"].get("predictions", {}).get(annotation_name, {})
        )

    if not visionai_dict:
        visionai_dict = gen_empty_vai(datarow=datarow, sequence_folder_url="")

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

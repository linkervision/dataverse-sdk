import asyncio
import json
import os
from collections.abc import AsyncGenerator
from typing import Callable

import aiohttp
from tqdm import tqdm
from visionai_data_format.utils.common import ANNOT_PATH

from .base import ExportAnnotationBase
from .constant import BATCH_SIZE, MAX_CONCURRENT_DOWNLOADS, ExportFormat
from .exporter import Exporter
from .utils import convert_to_bytes

VLM_ANNOTATION_FILE = "vlm_annotation.json"


@Exporter.register(format=ExportFormat.VLM)
class ExportVQA(ExportAnnotationBase):
    async def download_batch(
        self,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        batch_datarows: list[dict],
    ) -> list[tuple[bytes, str]]:
        tasks = []

        for datarow in batch_datarows:
            url = datarow["url"]
            file_path = os.path.join("images", datarow["unique_file_name"])

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
        question_id_map: dict,
        datarow_generator_func: Callable[[list], AsyncGenerator[dict]],
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
        image_id = f"{image_id_start:012d}"
        vlm_annotation = datarow["vlm_items"]["data"]
        vlm_annotation["id"] = image_id
        vlm_annotation["image"] = datarow["unique_file_name"]
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

    return json.dumps(annotation_list, ensure_ascii=False)

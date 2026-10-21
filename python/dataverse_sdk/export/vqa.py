import asyncio
import json
import logging
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

logger = logging.getLogger(__name__)

VLM_ANNOTATION_FILE = "vlm_annotation.json"
LLAVA_ANNOTATION_FILE = "llava_annotation.json"
LLAVA_IMAGE_TOKEN = "<image>"


@Exporter.register(format=ExportFormat.VLM)
class ExportVQA(ExportAnnotationBase):
    annotation_file = VLM_ANNOTATION_FILE

    @staticmethod
    def convert(
        datarows: list[dict],
        annotation_name: str,
        question_id_map: dict,
        written_files: set,
    ) -> str:
        return convert_annotation(
            datarows=datarows,
            annotation_name=annotation_name,
            question_id_map=question_id_map,
        )

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
            written_files = set()  # Basenames of images actually written to disk
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
                                    written_files.add(os.path.basename(result[1]))
                                    yield result
                                    progress_bar.update(1)
                            current_batch = []

                if current_batch:
                    results = await self.download_batch(
                        session, semaphore, current_batch
                    )
                    for result in results:
                        if result:
                            written_files.add(os.path.basename(result[1]))
                            yield result
                            progress_bar.update(1)

        annot_bytes = convert_to_bytes(
            self.convert(
                datarows=datarows,
                annotation_name=annotation_name,
                question_id_map=question_id_map,
                written_files=written_files,
            )
        )
        yield annot_bytes, os.path.join(ANNOT_PATH, self.annotation_file)


@Exporter.register(format=ExportFormat.LLAVA)
class ExportLLaVA(ExportVQA):
    annotation_file = LLAVA_ANNOTATION_FILE

    @staticmethod
    def convert(
        datarows: list[dict],
        annotation_name: str,
        question_id_map: dict,
        written_files: set,
    ) -> str:
        return convert_llava_annotation(
            datarows=datarows,
            annotation_name=annotation_name,
            question_id_map=question_id_map,
            written_files=written_files,
        )


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


def convert_vlm_answer(answer) -> str:
    """Cast a VLM answer back to LLaVA text: boolean -> Yes/No, option list -> its values, number -> str."""
    if isinstance(answer, bool):
        return "Yes" if answer else "No"
    if isinstance(answer, list):
        return ", ".join(str(value) for value in answer)
    return str(answer)


def convert_llava_annotation(
    datarows: list[dict], annotation_name: str, question_id_map: dict, written_files: set
) -> str:
    """Convert VLM datarows to LLaVA 1.5 items; each turn keeps the VLM question_id in `meta`."""
    llava_items = []
    for datarow in datarows:
        # The image failed to download, so the item has nothing to point at
        if datarow.get("unique_file_name") not in written_files:
            logger.warning("Skip datarow %s: image not exported", datarow["id"])
            continue
        vlm_items = datarow.get("vlm_items") or {}
        turns = []
        for conversation in vlm_items.get("data", {}).get("conversations", []):
            question_id = conversation["question_id"]
            answer = conversation["answer"].get(annotation_name)
            if answer is None:
                continue
            question = question_id_map.get(question_id)
            if question is None:
                logger.warning(
                    "Skip question_id %s of datarow %s: not in project ontology",
                    question_id,
                    datarow["id"],
                )
                continue
            if not turns:
                question = f"{LLAVA_IMAGE_TOKEN}\n{question}"
            meta = {"question_id": question_id}
            turns.append({"from": "human", "value": question, "meta": meta})
            turns.append({"from": "gpt", "value": convert_vlm_answer(answer), "meta": meta})

        llava_items.append(
            {
                "id": f"{len(llava_items):012d}",
                "image": datarow["unique_file_name"],
                "conversations": turns,
            }
        )
    return json.dumps(llava_items, ensure_ascii=False)

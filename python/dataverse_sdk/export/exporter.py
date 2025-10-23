import asyncio
import os
from collections import defaultdict
from collections.abc import AsyncGenerator, Generator
from typing import Callable

import aiofiles

from ..apis.backend import AsyncBackendAPI
from ..schemas.format import AnnotationFormat
from .base import ExportAnnotationBase
from .constant import BATCH_SIZE, MAX_CONCURRENT_DOWNLOADS, ExportFormat

NONE_SEQUENCE_DATAROW_ID = -1


class Exporter:
    export_annotation_m: dict[str, ExportAnnotationBase] = {}

    def __init__(
        self,
        target_folder: str,
        async_api: AsyncBackendAPI,
    ):
        self.target_folder: str = target_folder
        self.curation_api: AsyncBackendAPI = async_api

        # prepare stage parameter
        self.export_annot: ExportAnnotationBase = None

    async def prepare(
        self,
        dataslice_id: int,
        is_sequential: bool,
        export_format: ExportFormat,
    ) -> tuple[list[str], dict, dict]:
        if export_format not in self.export_annotation_m:
            raise ValueError(
                f"missing export annotation base for format {export_format}"
            )
        self.export_annot: ExportAnnotationBase = self.export_annotation_m[
            export_format
        ]
        dataslice: dict = await self.curation_api.get_dataslice(
            dataslice_id=dataslice_id
        )
        sequence_frame_map: dict[
            int, dict[int, list]
        ] = await get_datarows_sequence_info(
            curation_api=self.curation_api,
            dataslice_id=dataslice_id,
            dataslice_type=dataslice["type"],
        )
        new_sequence_frame_map: dict[int, dict[int, list[int]]] = get_datarow_sequences(
            is_sequential=is_sequential,
            sequence_frame_map=sequence_frame_map,
        )

        class_names: list[str] = [
            obj["name"] for obj in dataslice["project"]["ontology"]["classes"]
        ]

        project_id = dataslice["project"]["id"]
        question_id_map = {}
        if export_format == AnnotationFormat.VLM:
            project: dict = await self.curation_api.get_project(project_id=project_id)
            for question in project["ontology"]["classes"]:
                question_id_map[question["rank"]] = question["extended_class"][
                    "question"
                ]

        return (class_names, new_sequence_frame_map, question_id_map)

    async def producer(
        self,
        class_names: list[str],
        sequence_frame_map: dict,
        question_id_map: dict,
        annotation_name: str,
        is_sequential: bool,
    ) -> AsyncGenerator[tuple[bytes, str]]:
        async for data, path in self.export_annot.producer(
            class_names=class_names,
            sequence_frame_map=sequence_frame_map,
            question_id_map=question_id_map,
            target_folder=self.target_folder,
            datarow_generator_func=await self._gen(self.curation_api),
            annotation_name=annotation_name,
            is_sequential=is_sequential,
        ):
            if not path:
                continue
            yield data, path

    async def consumer(
        self,
        producer: Generator[tuple[bytes, str]],
    ):
        counter = 0
        current_batch = []
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

        async def write_file(bytes_: bytes, file_path: str):
            async with semaphore:
                dir_path = os.path.dirname(file_path)
                os.makedirs(dir_path, exist_ok=True)
                async with aiofiles.open(file_path, "wb") as f:
                    await f.write(bytes_)

        async for bytes_, path in producer:
            full_path = os.path.join(self.target_folder, path)
            current_batch.append((bytes_, full_path))

            if len(current_batch) >= BATCH_SIZE:
                tasks = [write_file(b, p) for b, p in current_batch]
                await asyncio.gather(*tasks)

                counter += len(current_batch)

                current_batch = []

        if current_batch:
            tasks = [write_file(b, p) for b, p in current_batch]
            await asyncio.gather(*tasks)
            counter += len(current_batch)

        print(f"Total files processed: {counter}")

    @classmethod
    def register(cls, format: ExportFormat):
        def _wrap(export_annotation: ExportAnnotationBase):
            cls.export_annotation_m[format] = export_annotation()  # register instance

        return _wrap

    @staticmethod
    async def _gen(
        curation_api: AsyncBackendAPI,
    ) -> Callable[[list[int]], AsyncGenerator[dict]]:
        async def f(datarow_id_list: list[int]):
            datarow_id_set: set[int] = set()
            for id_chunks in chunks(datarow_id_list, 1000):
                gen: AsyncGenerator = curation_api.get_datarows(
                    id_set_list=id_chunks,
                    batch_size=BATCH_SIZE,
                    fields="id,items,vlm_items,url,frame_id,image_width,image_height,sensor_name,original_url,type",
                )
                async for batched_datarow in gen:
                    for datarow in batched_datarow:
                        dr_id = datarow["id"]
                        if dr_id in datarow_id_set:
                            # drop duplicate datarows
                            continue
                        datarow_id_set.add(dr_id)
                        yield datarow

        return f

    def get_unique_filename(self, original_file_name, existing_files):
        base_name, extension = os.path.splitext(original_file_name)
        counter = 1
        new_file_name = original_file_name

        while new_file_name in existing_files:
            new_file_name = f"{base_name}({counter}){extension}"
            counter += 1

        return new_file_name


async def get_datarows_sequence_info(
    curation_api: AsyncBackendAPI, dataslice_id: int, dataslice_type: str
) -> dict[int, dict[int, list[int]]]:
    """Retrieve datarows dataset_id,sequence_id,frame_id info

    Parameters
    ----------
    curation_api : AsyncBackendAPI
        backend api
    dataslice_id : int
        current dataslice id
    dataslice_type : str
        the dataslice_type for condition to retrieve datarows

    Returns
    -------
    dict[int, dict[int, list]]
        datarow map with sequence_datarow_id as key and the frame_datarow_id as sub-key with list of datarow ids
    """
    # Call datarows_flat_parent for building sequence/frame structure
    # Get frame_datarow_id as a unique indicator
    if dataslice_type not in {"image", "pcd"}:
        dataslice_type = "base"

    datarows_generator: Generator = curation_api.get_datarows_flat_parent(
        batch_size=BATCH_SIZE,
        dataslice_id=dataslice_id,
        type=dataslice_type,
        fields="id,sequence_datarow_id,frame_datarow_id",
    )

    datarow_retrieved_info: dict[int, dict[int, list[int]]] = defaultdict(
        lambda: defaultdict(list)
    )
    async for datarows in datarows_generator:
        for datarow in datarows:
            # set NONE_SEQUENCE_DATAROW_ID for None case (which mean the original data is not sequential)
            if datarow["sequence_datarow_id"] is None:
                datarow["sequence_datarow_id"] = NONE_SEQUENCE_DATAROW_ID
            datarow_retrieved_info[datarow["sequence_datarow_id"]][
                int(datarow["frame_datarow_id"])
            ].append(datarow["id"])
    return datarow_retrieved_info


def get_datarow_sequences(
    sequence_frame_map: dict[int, dict[int, list[int]]],
    is_sequential: bool,
) -> dict[int, dict[int, list[int]]]:
    """reorder new sequence from current datarows

    Parameters
    ----------
    datarows_sequence_frame_map : dict[int, dict[int, list]]
        datarows info with dataset_id and sequence_id as its key
    is_sequential : bool
        flag to keep sequential of datarows

    Returns
    -------
    dict[int, dict[int, list]]
        dictionary of sequence order as key and frame datarow_id as subkey with its related datarow ids
    """
    new_datarows_sequence_map: dict[int, dict[int, list]] = defaultdict(
        lambda: defaultdict(list)
    )

    sequence_order = 0
    for sequence_datarow_id in sequence_frame_map.keys():
        for frame_datarow_id, datarow_id_list in sequence_frame_map[
            sequence_datarow_id
        ].items():
            new_datarows_sequence_map[sequence_order][frame_datarow_id] = (
                datarow_id_list
            )
            if not is_sequential or sequence_datarow_id == NONE_SEQUENCE_DATAROW_ID:
                sequence_order += 1
        if is_sequential:
            sequence_order += 1
    return new_datarows_sequence_map


def chunks(lst: list, n: int):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]

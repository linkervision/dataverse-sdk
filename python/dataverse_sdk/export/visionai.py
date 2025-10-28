import asyncio
import copy
import os
import uuid
from collections import defaultdict
from collections.abc import AsyncGenerator
from typing import Callable, Optional

import aiohttp
from tqdm import tqdm
from visionai_data_format.schemas.visionai_schema import VisionAIModel

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


def merge_intervals(intervals: list[tuple[int, int]]):
    # merge intervals in case there is any interval that could overlap
    # [(0, 3), (3, 5), (8, 9), (12, 12)] -> [(0, 5), (8, 9), (12, 12)]
    if not intervals:
        return []

    # Step 1: Sort intervals based on the start time
    intervals.sort(key=lambda x: x[0])

    # Step 2: Initialize merged list and add the first interval
    merged = [intervals[0]]

    # Step 3: Merge overlapping intervals
    for interval in intervals[1:]:
        last_merged_interval = merged[-1]

        # Check if the current interval overlaps with the last interval in the merged list
        if interval[0] - last_merged_interval[1] > 1:
            # If there is no overlap, simply add the current interval to the merged list
            merged.append(interval)
            continue

        # Merge the intervals by updating the end time of the last interval in the merged list
        last_merged_interval = (
            last_merged_interval[0],
            max(last_merged_interval[1], interval[1]),
        )
        merged[-1] = last_merged_interval

    # Step 4: Return the merged list of intervals
    return merged


def gen_intervals(range_list: list[int]) -> list[tuple[int, int]]:
    """given a list of numbers, return its range interval list

    Parameters
    ----------
    range_list : list[int]
        list of numbers

    Returns
    -------
    list[tuple[int, int]]
        list of range intervals in tuple, where first index is the start of the range,
        the second index is the end of the range
    """
    # generate intervals from list
    # [0,1,2,3,5,8,9,12] -> [(0, 3), (5, 5), (8, 9), (12, 12)]
    range_list.sort()
    start, end = range_list[0], range_list[0]
    result_intervals: list[tuple[int, int]] = [(start, end)]
    for frame_num in range_list:
        last_start, last_end = result_intervals[-1]
        if last_start <= frame_num <= last_end:
            continue
        if frame_num > last_end and frame_num - last_end == 1:
            result_intervals[-1] = (last_start, frame_num)
        elif frame_num < last_start and last_start - frame_num == 1:
            result_intervals[-1] = (frame_num, last_end)
        else:
            result_intervals.append((frame_num, frame_num))

    if len(result_intervals) == 1:
        return result_intervals

    return merge_intervals(result_intervals)


def combine_static_dynamic_data(
    frame_num_set: set[int],
    sub_root_key: str = "object_data_pointers",
    static_data: Optional[dict[str, list[dict]]] = None,
    dynamic_data: Optional[dict] = None,
) -> Optional[dict]:
    if not static_data and not dynamic_data:
        return None

    # combine objects data and its interval
    combined_objects = {}
    for data_uuid, data_info_list in static_data.items():
        for _, data_info in data_info_list:
            # initialize combined objects with given data uuid
            # since we don't have the data of given uuid in our combined objects
            if data_uuid not in combined_objects:
                combined_objects[data_uuid] = data_info
                continue
            # add frame intervals information for given data uuid
            for data_pointer_name, data_pointer_info in data_info[sub_root_key].items():
                if data_pointer_name not in combined_objects[data_uuid][sub_root_key]:
                    combined_objects[data_uuid][sub_root_key].update(
                        {data_pointer_name: data_pointer_info}
                    )
                    continue
                combined_objects[data_uuid][sub_root_key][data_pointer_name][
                    "frame_intervals"
                ] += data_pointer_info["frame_intervals"]

    # check if the objects are required since we decreasing the frame number
    # if the objects don't required, we remove them from combined objects to be assigned into visionai
    for obj_uuid, obj_data in list(combined_objects.items()):
        for data_pointers_info in obj_data[sub_root_key].values():
            current_obj_frame_list: list[int] = [
                i
                for frame_interval in data_pointers_info["frame_intervals"]
                for i in range(
                    int(frame_interval["frame_start"]),
                    int(frame_interval["frame_end"]) + 1,
                )
            ]
            current_obj_frame_set = set(current_obj_frame_list)

            allowed_obj_frame_list: list[int] = list(
                current_obj_frame_set & frame_num_set
            )
            # remove current object uuid if current frame list doesn't contain the object
            if not allowed_obj_frame_list:
                del combined_objects[obj_uuid]
                continue

            data_pointers_info["frame_intervals"] = [
                {"frame_start": data[0], "frame_end": data[1]}
                for data in gen_intervals(allowed_obj_frame_list)
            ]

    # combine object intervals
    dynamic_uuid_list: list[str] = []
    if dynamic_data:
        dynamic_uuid_list = list(dynamic_data.keys())
    uuid_list: list[str] = list(static_data.keys()) + dynamic_uuid_list
    for obj_uuid in uuid_list:
        if obj_uuid not in combined_objects:
            continue
        static_frame_list: list[int] = [int(data[0]) for data in static_data[obj_uuid]]
        dynamic_frame_list: list[int] = list(dynamic_data.get(obj_uuid, set()))
        combined_frame_list: list[int] = list(
            set(static_frame_list + dynamic_frame_list)
        )
        uuid_frame_interval = gen_intervals(combined_frame_list)
        combined_objects[obj_uuid]["frame_intervals"] = [
            {"frame_start": start, "frame_end": end}
            for start, end in uuid_frame_interval
        ]
    return combined_objects


def replace_data_frame_intervals(
    root_key: str, data_info: dict, frame_num: int
) -> None:
    """replace current objects under visionai frame intervlas

    Parameters
    ----------
    root_key : str
        current annotation object key such as "contexts" or "objects"
    data_info : dict
        annotation data
    frame_num : int
        current frame number
    """
    sub_root_key_map = {
        "contexts": "context_data_pointers",
        "objects": "object_data_pointers",
    }
    sub_root_key = sub_root_key_map[root_key]
    new_frame_intervals = [{"frame_start": frame_num, "frame_end": frame_num}]
    for pointer in data_info[sub_root_key].values():
        pointer["frame_intervals"] = new_frame_intervals


def aggregate_static_annotations(
    datarows: list[dict], root_key: str, annotation_name: str
) -> defaultdict[str, list[tuple[int, dict]]]:
    """aggregate static annotations of visionai data

    Parameters
    ----------
    datarows : list[dict]
        list of datarow
    root_key : str
        current annotation object key such as "contexts" or "objects"
    annotation_name: str
        specified annotation name

    Returns
    -------
    dict[str, list[tuple[int, dict]]]
        a map of object uuid with its frame and annotation info
    """
    large_data: dict[str, list[tuple[int, dict]]] = defaultdict(list)
    for datarow in datarows:
        datarow_items = datarow["items"]
        frame_num = int(datarow["frame_id"])
        if annotation_name == GROUNDTRUTH:
            data = datarow_items.get(GROUND_TRUTH_ANNOTATION_NAME, {}).get(root_key, {})
        else:
            data = datarow_items.get("predictions", {}).get(annotation_name, {})

        for data_uuid, data_info in data.items():
            if root_key in {"contexts", "objects"}:
                replace_data_frame_intervals(root_key, data_info, frame_num)
            large_data[data_uuid].append((frame_num, data_info))
    return large_data


def update_streams_uri(
    streams: dict, sequence_folder_url: str, original_file_name: Optional[str] = None
) -> dict:
    """Update streams under frames uri

    Example:
    stream old uri path is the path of current frame/datarow saved in our dataverse container
    : "https://dataverse-container-local.s3.ap-northeast-1.amazonaws.com/backend/upload/datasets
        /vainewformat-20230801042356724969/data/000000000004/data/camera1/000000000000.jpg"

    since we only need to keep the frame number for each new sequence,
    we only take last three path from uri : "data/camera1/000000000000.jpg"

    then we could generate new uri path for the exported frame/datarow

    Parameters
    ----------
    streams : dict
        streams data contains multiple sensors and its uri
    sequence_folder_url : str
        sequence folder url destination
    original_file_name: Optional[str]
        original file name for the given image/pcd


    Returns
    -------
    dict
        updated streams data
    """

    current_streams = copy.deepcopy(streams)
    for stream_data in current_streams.values():
        old_uri_path_list = stream_data["uri"].split("/")
        file_path = old_uri_path_list[-3:]
        stream_data["uri"] = os.path.join(sequence_folder_url, *file_path)
        if original_file_name is not None:
            stream_data["original_file_name"] = original_file_name
    return current_streams


def aggregate_visionai_tags(
    datarows: list[dict], frames: dict[str, dict], annotation_name: str
) -> tuple[dict, dict]:
    new_frames = copy.deepcopy(frames)

    # 1. combine all tags classes from different datarows
    sequence_class_set = set()
    for datarow in datarows:
        if annotation_name == GROUNDTRUTH:
            current_tags = (
                datarow["items"].get(GROUND_TRUTH_ANNOTATION_NAME, {}).get("tags")
            )
        else:
            current_tags = (
                datarow["items"]
                .get("predictions", {})
                .get(annotation_name, {})
                .get("tags")
            )

        if not current_tags:
            continue

        tag_data = None
        try:
            current_tag: dict = next(iter(current_tags.values()))
            tag_data = current_tag.get("tag_data", {})
        except StopIteration:
            pass
        if not tag_data:
            continue
        class_list = tag_data["vec"][0]["val"]
        sequence_class_set |= set(class_list)

    if not sequence_class_set:
        return {}, new_frames

    # we convert the set to list to keep the order
    sequence_class_list = list(sequence_class_set)

    # 2. create new class number map
    sequence_class_name_id_map = {
        cls_name: cls_idx for cls_idx, cls_name in enumerate(sequence_class_list)
    }

    # 3. replace each datarows RLE number with sequence class order
    for datarow in datarows:
        if annotation_name == GROUNDTRUTH:
            current_tags = (
                datarow["items"].get(GROUND_TRUTH_ANNOTATION_NAME, {}).get("tags")
            )
        else:
            current_tags = (
                datarow["items"]
                .get("predictions", {})
                .get(annotation_name, {})
                .get("tags")
            )

        tag_data = None
        try:
            current_tag_data: dict = next(iter(current_tags.values()))
            tag_data = current_tag_data.get("tag_data", {})
        except StopIteration:
            pass

        if not tag_data:
            continue

        frame_num = f"{int(datarow['frame_id']):012d}"
        frame_data = new_frames[frame_num]

        frame_object = None
        try:
            frame_object: dict = next(iter(frame_data.get("objects", {}).values()))
        except StopIteration:
            pass

        if not frame_object:
            continue

        annotation_class_list = tag_data["vec"][0]["val"]

        binary_data = frame_object["object_data"]["binary"]
        current_annotation_rle = binary_data[0]["val"]

        annotation_idx_name_map = {
            cls_idx: cls_name for cls_idx, cls_name in enumerate(annotation_class_list)
        }

        new_annotation_rle = ""
        for annotation_info in current_annotation_rle.split("#"):
            pixel_count_class = annotation_info.split("V")
            if len(pixel_count_class) != 2:
                continue
            pixel_count, pixel_class = pixel_count_class
            new_pixel_class = sequence_class_name_id_map[
                annotation_idx_name_map[int(pixel_class)]
            ]
            new_annotation_rle += f"#{pixel_count}V{new_pixel_class}"

        frame_object["object_data"]["binary"][0]["val"] = new_annotation_rle

    # 4. generate tags under visionai
    tags = {
        str(uuid.uuid4()): {
            "ontology_uid": "",
            "type": "semantic_segmentation_RLE",
            "tag_data": {
                "vec": [{"name": "", "type": "values", "val": sequence_class_list}]
            },
        }
    }
    return tags, new_frames


def aggregate_datarows_annotations(
    frame_datarows: dict[int, list[dict]],
    sequence_folder_url: str,
    annotation_name: str,
) -> dict:
    """Aggregate and generate visionai annotation data

    Parameters
    ----------
    frame_datarows : dict[int, list[dict]]
        frame mapping to list of datarow
    sequence_folder_url: str
        new sequence folder url
    annotation_name: str
        specified annotation name
    Returns
    -------
    dict
        visionai dictionary
    """
    # retrieve data under frames
    dynamic_object_uuid_frame_map: dict[str, set] = defaultdict(set)
    dynamic_contexts_uuid_frame_map: dict[str, set] = defaultdict(set)

    combined_frames: dict[str, dict] = {}

    coordinate_systems: Optional[dict] = None
    streams: dict = {}
    all_datarows = []
    for _, datarows in frame_datarows.items():
        all_datarows.extend(datarows)
        # update datarow visionai for same frame
        current_frame = {
            "objects": {},
            "contexts": {},
            "frame_properties": {"streams": {}},
        }
        for datarow in datarows:
            datarow_id: int = datarow["id"]
            datarow_items: dict = datarow["items"]
            frame_num = int(datarow["frame_id"])
            original_file_name = os.path.basename(datarow["original_url"])

            if annotation_name == GROUNDTRUTH:
                vai = copy.deepcopy(datarow_items.get(GROUND_TRUTH_ANNOTATION_NAME, {}))
            else:
                vai = copy.deepcopy(
                    datarow_items.get("predictions", {}).get(annotation_name, {})
                )

            if not vai:
                vai = gen_empty_vai(
                    datarow=datarow, sequence_folder_url=sequence_folder_url
                )

            # we could retrieve the first data of frames under items
            # since each items inside datarow contains only one frames
            try:
                _, frame = vai.get("frames", {}).popitem()
            except KeyError:
                raise ValueError(
                    f"Current datarow {datarow_id} items have no frames data"
                )  # noqa: B904
            frame_objects = frame.get("objects", {})
            if frame_objects:
                for obj_uuid, obj_data in frame_objects.items():
                    dynamic_object_uuid_frame_map[obj_uuid].add(frame_num)
                    if obj_uuid not in current_frame["objects"]:
                        current_frame["objects"][obj_uuid] = obj_data
                    else:
                        for obj_type, obj_list in obj_data["object_data"].items():
                            if current_frame["objects"][obj_uuid]["object_data"].get(
                                obj_type
                            ):
                                current_frame["objects"][obj_uuid]["object_data"][
                                    obj_type
                                ].extend(obj_list)
                            else:
                                current_frame["objects"][obj_uuid][
                                    "object_data"
                                ].update({obj_type: obj_list})

            frame_contexts = frame.get("contexts", {})
            if frame_contexts:
                for ctx_uuid, ctx_data in frame_contexts.items():
                    dynamic_contexts_uuid_frame_map[ctx_uuid].add(frame_num)
                    if ctx_uuid not in current_frame["contexts"]:
                        current_frame["contexts"][ctx_uuid] = ctx_data
                    else:
                        for ctx_type, ctx_list in ctx_data.items():
                            if current_frame["contexts"][ctx_uuid]["context_data"].get(
                                ctx_type
                            ):
                                current_frame["contexts"][ctx_uuid]["context_data"][
                                    ctx_type
                                ].extend(ctx_list)
                            else:
                                current_frame["contexts"][ctx_uuid][
                                    "context_data"
                                ].update({ctx_type: ctx_list})

            if (
                "timestamp" in frame["frame_properties"]
                and "timestamp" not in current_frame["frame_properties"]
            ):
                current_frame["frame_properties"]["timestamp"] = frame[
                    "frame_properties"
                ]["timestamp"]
            # Update current frame uri
            current_frame["frame_properties"]["streams"].update(
                update_streams_uri(
                    streams=frame["frame_properties"]["streams"],
                    sequence_folder_url=sequence_folder_url,
                    original_file_name=original_file_name,
                )
            )
            # coordinate system can be optional in visionai
            if not coordinate_systems and "coordinate_systems" in vai:
                coordinate_systems = vai["coordinate_systems"]
            current_vai_sensor = next(iter(vai["streams"]))
            if current_vai_sensor not in streams:
                streams.update(
                    update_streams_uri(
                        streams=vai["streams"],
                        sequence_folder_url=sequence_folder_url,
                        original_file_name=None,
                    )
                )
        if not current_frame.get("objects"):
            current_frame.pop("objects", None)
        if not current_frame.get("contexts"):
            current_frame.pop("contexts", None)
        combined_frames[f"{frame_num:012d}"] = copy.deepcopy(current_frame)

    # list out all frames number of combined frames
    frame_num_set = {int(num) for num in combined_frames.keys()}
    # combine all objects under visionai
    static_objects_map: dict[str, list[tuple[int, dict]]] = (
        aggregate_static_annotations(
            datarows=all_datarows, root_key="objects", annotation_name=annotation_name
        )
    )

    # combine all contexts under visionai
    static_contexts_map: dict[str, list[tuple[int, dict]]] = (
        aggregate_static_annotations(
            datarows=all_datarows, root_key="contexts", annotation_name=annotation_name
        )
    )

    # retrieve tags under visionai
    # we could retrieve tags under visionai from any datarows
    # since tags values are equals for all frames under same sequence

    tags_under_visionai, combined_frames = aggregate_visionai_tags(
        frames=combined_frames, datarows=datarows, annotation_name=annotation_name
    )

    # combine static and dynamic objects
    combined_objects_map: Optional[dict] = combine_static_dynamic_data(
        static_data=static_objects_map,
        dynamic_data=dynamic_object_uuid_frame_map,
        frame_num_set=frame_num_set,
        sub_root_key="object_data_pointers",
    )

    # combine static and dynamic contexts
    combined_contexts_map: Optional[dict] = combine_static_dynamic_data(
        static_data=static_contexts_map,
        dynamic_data=dynamic_contexts_uuid_frame_map,
        frame_num_set=frame_num_set,
        sub_root_key="context_data_pointers",
    )

    frame_interval_list: list[tuple[int, int]] = gen_intervals(list(frame_num_set))
    visionai = {
        "frames": combined_frames,
        # we could assign frame_intervals directly since we rearrange all datarows order
        "frame_intervals": [
            {"frame_start": frame_start, "frame_end": frame_end}
            for frame_start, frame_end in frame_interval_list
        ],
        "streams": streams,
        "metadata": {"schema_version": "1.0.0"},
    }
    if coordinate_systems:
        visionai["coordinate_systems"] = coordinate_systems
    if combined_objects_map:
        visionai["objects"] = combined_objects_map
    if combined_contexts_map:
        visionai["contexts"] = combined_contexts_map
    if tags_under_visionai:
        visionai["tags"] = tags_under_visionai
    return VisionAIModel(**{"visionai": visionai}).model_dump(exclude_none=True)


@Exporter.register(format=ExportFormat.VISIONAI)
class ExportVisionAI(ExportAnnotationBase):
    async def download_batch(
        self,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        batch_datarows: list[dict],
        datarow_id_to_frame_datarow_id: dict[int, int],
        frame_datarow_id_to_sequence_id: dict[int, int],
    ) -> list[tuple[bytes, str]]:
        tasks = []
        for datarow in batch_datarows:
            url = datarow["url"]
            frame_num, sensor_name = int(datarow["frame_id"]), datarow["sensor_name"]
            frame_datarow_id = datarow_id_to_frame_datarow_id[datarow["id"]]
            sequence_id = frame_datarow_id_to_sequence_id[frame_datarow_id]
            file_name = url.split("/")[-1]
            file_path = os.path.join(
                f"{sequence_id:012d}",
                "data",
                sensor_name,
                f"{frame_num:012d}{os.path.splitext(file_name)[-1]}",
            )

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

    async def process_datarows(
        self,
        datarow_generator_func: Callable[[list], AsyncGenerator[dict]],
        datarow_id_list: list[int],
        frame_datarow_id_to_sequence_id: dict[int, int],
        sequence_frame_datarows: defaultdict[int, list[dict]],
        target_folder: str,
        annotation_name: str,
        datarow_id_to_frame_datarow_id: dict[int, int],
        current_batch: list[dict],
        pre_frame_datarow_id: int | None,
        last_batch: bool,
        is_sequential: bool,
    ) -> tuple[
        list[tuple[bytes, str]],
        defaultdict[int, list[dict]],
        list[int],
        int | None,
        list[dict],
    ]:
        annotation_results = []

        def create_aggregated_annotation(
            frame_datarows: dict, seq_id: int
        ) -> tuple[bytes, str]:
            """Helper to create aggregated annotation bytes and path."""
            annot_bytes = convert_to_bytes(
                aggregate_datarows_annotations(
                    frame_datarows=frame_datarows,
                    sequence_folder_url=os.path.join(
                        target_folder, f"{seq_id:012d}", ""
                    ),
                    annotation_name=annotation_name,
                )
            )
            anno_path = os.path.join(
                f"{seq_id:012d}", "annotations", annotation_name, "visionai.json"
            )
            return (annot_bytes, anno_path)

        if is_sequential:
            async for datarow in datarow_generator_func(datarow_id_list):
                frame_datarow_id = datarow_id_to_frame_datarow_id[datarow["id"]]
                sequence_frame_datarows[frame_datarow_id].append(datarow)
                current_batch.append(datarow)

            sequence_id = frame_datarow_id_to_sequence_id[frame_datarow_id]
            annotation_results.append(
                create_aggregated_annotation(sequence_frame_datarows, sequence_id)
            )
            sequence_frame_datarows = defaultdict(list)

            return (
                annotation_results,
                sequence_frame_datarows,
                datarow_id_list,
                pre_frame_datarow_id,
                current_batch,
            )

        async for datarow in datarow_generator_func(datarow_id_list):
            frame_datarow_id = datarow_id_to_frame_datarow_id[datarow["id"]]
            current_batch.append(datarow)
            if pre_frame_datarow_id is None:
                pre_frame_datarow_id = frame_datarow_id
                sequence_frame_datarows[frame_datarow_id].append(datarow)
            elif pre_frame_datarow_id != frame_datarow_id:
                # export previous frame when frame_datarow_id changes
                pre_sequence_id = frame_datarow_id_to_sequence_id[pre_frame_datarow_id]
                annotation_results.append(
                    create_aggregated_annotation(
                        sequence_frame_datarows, pre_sequence_id
                    )
                )
                sequence_frame_datarows.pop(pre_frame_datarow_id)
                sequence_frame_datarows[frame_datarow_id].append(datarow)
                pre_frame_datarow_id = frame_datarow_id

        if last_batch:
            sequence_id = frame_datarow_id_to_sequence_id[frame_datarow_id]
            annotation_results.append(
                create_aggregated_annotation(sequence_frame_datarows, sequence_id)
            )
            sequence_frame_datarows = defaultdict(list)

        return (
            annotation_results,
            sequence_frame_datarows,
            datarow_id_list,
            pre_frame_datarow_id,
            current_batch,
        )

    async def producer(
        self,
        target_folder: str,
        sequence_frame_map: dict[int, dict[int, list[int]]],
        datarow_generator_func: Callable[[list], AsyncGenerator[dict]],
        annotation_name: str,
        is_sequential: bool,
        *_,
        **kwargs,
    ) -> AsyncGenerator[bytes, str]:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
        async with aiohttp.ClientSession() as session:
            datarow_id_list = []
            total_datarows = sum(len(v) for v in sequence_frame_map.values())
            annotation_results = []
            current_batch = []
            pre_frame_datarow_id = None
            frame_datarow_id_to_sequence_id = {}
            datarow_id_to_frame_datarow_id = {}
            with tqdm(
                total=total_datarows, desc="Downloading images", unit="file"
            ) as progress_bar:
                sequence_frame_datarows = defaultdict(list)
                for sequence_id, frame_datarow_map in sequence_frame_map.items():
                    for frame_datarow_id, datarow_ids in frame_datarow_map.items():
                        datarow_id_list.extend(datarow_ids)
                        frame_datarow_id_to_sequence_id[frame_datarow_id] = sequence_id
                        for datarow_id in datarow_ids:
                            datarow_id_to_frame_datarow_id[datarow_id] = (
                                frame_datarow_id
                            )
                        if not is_sequential and len(datarow_id_list) >= BATCH_SIZE:
                            (
                                annotation_results,
                                sequence_frame_datarows,
                                datarow_id_list,
                                pre_frame_datarow_id,
                                current_batch,
                            ) = await self.process_datarows(
                                datarow_generator_func,
                                datarow_id_list,
                                frame_datarow_id_to_sequence_id,
                                sequence_frame_datarows,
                                target_folder,
                                annotation_name,
                                datarow_id_to_frame_datarow_id,
                                current_batch,
                                pre_frame_datarow_id,
                                last_batch=False,
                                is_sequential=is_sequential,
                            )
                            results = await self.download_batch(
                                session,
                                semaphore,
                                current_batch,
                                datarow_id_to_frame_datarow_id,
                                frame_datarow_id_to_sequence_id,
                            )
                            for result in results:
                                if result:
                                    yield result
                                    progress_bar.update(1)
                            current_batch = []
                            datarow_id_list = []

                            for annotation_result in annotation_results:
                                yield annotation_result
                            annotation_results = []
                    if is_sequential:
                        # process sequence
                        (
                            annotation_results,
                            sequence_frame_datarows,
                            datarow_id_list,
                            pre_frame_datarow_id,
                            current_batch,
                        ) = await self.process_datarows(
                            datarow_generator_func,
                            datarow_id_list,
                            frame_datarow_id_to_sequence_id,
                            sequence_frame_datarows,
                            target_folder,
                            annotation_name,
                            datarow_id_to_frame_datarow_id,
                            current_batch,
                            pre_frame_datarow_id,
                            last_batch=False,
                            is_sequential=is_sequential,
                        )
                        # download sequence
                        results = await self.download_batch(
                            session,
                            semaphore,
                            current_batch,
                            datarow_id_to_frame_datarow_id,
                            frame_datarow_id_to_sequence_id,
                        )
                        for result in results:
                            if result:
                                yield result
                                progress_bar.update(1)
                        current_batch = []
                        datarow_id_list = []
                        for annotation_result in annotation_results:
                            yield annotation_result
                        annotation_results = []

                # update for non-sequential last batch
                if datarow_id_list:
                    (
                        annotation_results,
                        sequence_frame_datarows,
                        datarow_id_list,
                        pre_frame_datarow_id,
                        current_batch,
                    ) = await self.process_datarows(
                        datarow_generator_func,
                        datarow_id_list,
                        frame_datarow_id_to_sequence_id,
                        sequence_frame_datarows,
                        target_folder,
                        annotation_name,
                        datarow_id_to_frame_datarow_id,
                        current_batch,
                        pre_frame_datarow_id,
                        last_batch=True,
                        is_sequential=is_sequential,
                    )
                    results = await self.download_batch(
                        session,
                        semaphore,
                        current_batch,
                        datarow_id_to_frame_datarow_id,
                        frame_datarow_id_to_sequence_id,
                    )
                    for result in results:
                        if result:
                            yield result
                            progress_bar.update(1)

                if annotation_results:
                    for annotation_result in annotation_results:
                        yield annotation_result

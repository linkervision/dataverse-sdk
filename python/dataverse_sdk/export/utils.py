import json
from typing import Union

from visionai_data_format.schemas.visionai_schema import (
    Frame,
    FrameProperties,
    FramePropertyStream,
)

from ..apis.third_party import ThirdPartyAPI


def convert_to_bytes(obj: Union[dict, list, str]) -> bytes:
    if isinstance(obj, (dict, list)):
        jstr = json.dumps(obj)
    elif isinstance(obj, str):
        jstr = obj
    else:
        raise TypeError("un-support type")
    return bytes(jstr, encoding="utf8")


async def download_url_file_async(data_url: str) -> bytes | None:
    # get data from url link
    try:
        data: bytes = await ThirdPartyAPI.async_download_file(
            url=data_url, method="GET"
        )
    except Exception:
        print(f"Retrieving data from url {data_url} error")
        return None
    return data


def gen_empty_vai(datarow: dict, sequence_folder_url: str) -> dict:
    new_sensor_data_folder = f"{sequence_folder_url}/data/{datarow['sensor_name']}/"
    dest_url = f"{new_sensor_data_folder}{datarow['url'].split('/')[-1]}"

    # generate visionai empty frame
    frames = {}
    frame_num = datarow["frame_id"]
    frames[frame_num] = Frame(
        frame_properties=FrameProperties(
            streams={datarow["sensor_name"]: FramePropertyStream(uri=dest_url)}
        ),
        objects={},
    ).model_dump(exclude_none=True)
    if datarow["type"] == "image":
        stream = {datarow["sensor_name"]: {"type": "camera", "uri": dest_url}}
    else:
        stream = {datarow["sensor_name"]: {"type": "lidar", "uri": dest_url}}

    return {"frames": frames, "streams": stream}

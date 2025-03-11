import json
from typing import Union

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

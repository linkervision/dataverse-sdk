import argparse
import asyncio
import logging
from pathlib import Path

from dataverse_sdk.client import DataverseClient

logging.basicConfig(level=logging.INFO, format="%(message)s")

GLOBAL_MEAN_THRESHOLD_MIN = 0.000001
GLOBAL_MEAN_THRESHOLD_MAX = 0.01
PER_PATCH_256_MIN_THRESHOLD_MIN = 0.000001
PER_PATCH_256_MIN_THRESHOLD_MAX = 0.0001
SPLIT_DURATION_MIN = 2
SPLIT_DURATION_MAX = 30


def make_parser():
    parser = argparse.ArgumentParser("Upload videos and create session task")
    parser.add_argument(
        "-host",
        "--host",
        required=True,
        type=str,
        help="the host url of the dataverse site (with curation port)",
    )
    parser.add_argument(
        "-s",
        "--service_id",
        required=True,
        type=str,
        help="The service id of the dataverse you want to connect",
    )
    parser.add_argument(
        "-e",
        "--email",
        required=True,
        type=str,
        help="the email account of your dataverse workspace",
    )
    parser.add_argument(
        "-p",
        "--password",
        required=True,
        type=str,
        help="the password of your dataverse workspace",
    )
    parser.add_argument(
        "--folder",
        type=str,
        required=True,
        help="the local videos folder path for uploading",
    )
    parser.add_argument(
        "-n",
        "--name",
        type=str,
        default=None,
        help="the name for the session task (defaults to folder name)",
    )
    parser.add_argument(
        "--video-curation",
        action="store_true",
        default=False,
        help="enable video curation",
    )
    parser.add_argument(
        "--global-mean-threshold",
        type=float,
        default=0.001,
        help=f"Threshold for the video's global average motion magnitude ({GLOBAL_MEAN_THRESHOLD_MIN} ~ {GLOBAL_MEAN_THRESHOLD_MAX}). Higher values are stricter (flag more clips as low-motion); lower values are looser (flag fewer clips).",
    )
    parser.add_argument(
        "--per-patch-256-min-threshold",
        type=float,
        default=0.000001,
        help=f"Minimum average motion magnitude allowed in any 256x256 pixel patch ({PER_PATCH_256_MIN_THRESHOLD_MIN} ~ {PER_PATCH_256_MIN_THRESHOLD_MAX}). Higher values are stricter per-patch (flag more clips when any 256x256 patch is too still); lower values are looser (flag fewer clips).",
    )
    parser.add_argument(
        "--split-duration",
        type=int,
        default=5,
        help=f"Set the length of each split clip in seconds ({SPLIT_DURATION_MIN} ~ {SPLIT_DURATION_MAX}s).",
    )
    return parser.parse_args()


async def main():
    args = make_parser()

    video_folder = Path(args.folder)
    if not video_folder.exists():
        raise ValueError(f"Video folder does not exist: {args.folder}")

    # Use folder name as default session task name
    session_name = args.name if args.name else video_folder.name

    curation_config = None
    if args.video_curation:
        curation_config = {
            "global_mean_threshold": args.global_mean_threshold,
            "per_patch_256_min_threshold": args.per_patch_256_min_threshold,
            "split_duration": args.split_duration,
        }

    try:
        logging.info(f"Connecting to {args.host}...")
        client = DataverseClient(
            host=args.host,
            email=args.email,
            password=args.password,
            service_id=args.service_id,
        )

        logging.info(f"Creating session task: {session_name}")
        await client.create_session_task(
            name=session_name,
            video_folder=args.folder,
            video_curation=args.video_curation,
            curation_config=curation_config,
        )

    except Exception as e:
        logging.error(f"❌ Failed to create session task: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())

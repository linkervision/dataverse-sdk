import argparse
import asyncio
import os

from dataverse_sdk.apis.backend import AsyncBackendAPI
from dataverse_sdk.constants import DataverseHost
from dataverse_sdk.export.exporter import Exporter


def export_dataslice_to_local(
    host: str,
    email: str,
    password: str,
    service_id: str,
    dataslice_id: int,
    export_format: str,
    annotation_name: str = "",
    target_folder: str = "./",
    sequential: bool = False,
    alias: str = "default",
):
    if not os.path.exists(target_folder):
        os.makedirs(target_folder, exist_ok=True)
        # raise OSError(f"target folder: {target_folder} not exists")

    if not os.access(target_folder, os.W_OK):
        print(f"Write permission denied: {target_folder}")
        raise OSError(f"write permission denied {target_folder}")

    client = AsyncBackendAPI(
        host=host,
        email=email,
        password=password,
        service_id=service_id,
    )
    exporter: Exporter = Exporter(
        target_folder=target_folder,
        async_api=client,
    )

    async def run_export(exporter: Exporter):
        data: tuple[list[str], dict] = await exporter.prepare(
            dataslice_id=dataslice_id,
            is_sequential=sequential,
            export_format=export_format,
        )
        producer = exporter.producer(*data, annotation_name)
        await exporter.consumer(
            producer=producer,
        )

    asyncio.run(run_export(exporter))


def make_parser():
    parser = argparse.ArgumentParser("Export Dataslice data")
    parser.add_argument(
        "-host",
        "--host",
        required=True,
        type=str,
        help="the host url of the dataverse site (with curation port)",
        default=DataverseHost.STAGING.value,
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
        "-dataslice",
        "--dataslice_id",
        type=str,
        required=True,
        help="The project id you want to import dataset",
    )
    parser.add_argument(
        "--target_folder",
        type=str,
        help="the local data folder root folder for importing",
    )
    parser.add_argument(
        "--sequential",
        default=False,
        action="store_true",
        help="Whether dataset are sequential",
    )
    parser.add_argument(
        "--anno",
        type=str,
        default="groundtruth",
        help="the annotation name for exporting",
    )
    parser.add_argument(
        "--export-format",
        type=str,
        default="visionai",
        help="the export format",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = make_parser()

    # check if target_folder_exists

    export_dataslice_to_local(
        host=args.host,
        email=args.email,
        password=args.password,
        service_id=args.service_id,
        dataslice_id=args.dataslice_id,
        target_folder=args.target_folder,
        sequential=args.sequential,
        annotation_name=args.anno,
        export_format=args.export_format,
    )

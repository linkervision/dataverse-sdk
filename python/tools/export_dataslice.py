import argparse
import time

from dataverse_sdk import DataverseClient
from dataverse_sdk.constants import DataverseHost


def export_dataslice_to_local(
    host: str,
    email: str,
    password: str,
    service_id: str,
    dataslice_id: int,
    annotation_name: str = "",
    save_path: str = "./data.zip",
    sequential: bool = False,
    alias: str = "default",
):
    client = DataverseClient(
        host=host,
        email=email,
        password=password,
        service_id=service_id,
        alias=alias,
    )
    export_record = client.export_dataslice(
        dataslice_id=dataslice_id, annotation_name=annotation_name
    )
    export_record_id = export_record["export_record_id"]
    # get process status
    while True:
        # download the export data by export record id
        download: bool = client.download_export_dataslice_data(
            dataslice_id=dataslice_id,
            export_record_id=export_record_id,
            save_path=save_path,
        )
        if download:
            break
        print("-------- Wait for download process --------")
        time.sleep(60)


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
        "-f",
        "--file",
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
        default="",
        help="the annotation name for exporting",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = make_parser()
    output_file_name = args.file
    if not output_file_name:
        output_file_name = f"./{time.strftime('%Y-%m-%d_%H-%M-%S')}.zip"
    export_dataslice_to_local(
        host=args.host,
        email=args.email,
        password=args.password,
        service_id=args.service_id,
        dataslice_id=args.dataslice_id,
        save_path=output_file_name,
        sequential=args.sequential,
        annotation_name=args.anno,
    )

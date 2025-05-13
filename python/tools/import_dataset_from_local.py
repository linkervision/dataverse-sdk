import argparse
import logging
import time
from typing import Optional

from dataverse_sdk import DataverseClient
from dataverse_sdk.constants import DataverseHost
from dataverse_sdk.exceptions.client import DataverseExceptionBase
from dataverse_sdk.schemas.client import Project
from dataverse_sdk.schemas.common import AnnotationFormat, DatasetType, DataSource

AUTO_TAGGING_CLASSES = ["weather", "scene", "timeofday"]


def import_dataset_from_local(
    host: str,
    email: str,
    password: str,
    service_id: str,
    project_id: int,
    data_folder: str,
    dataset_name: str,
    dataset_type: DatasetType,
    annotation_format: AnnotationFormat,
    sequential: bool = False,
    gen_metadata: bool = False,
    gen_auto_tagging: bool = False,
    reupload_dataset_uuid: Optional[str] = None,
    alias: str = "default",
):
    client = DataverseClient(
        host=host,
        email=email,
        password=password,
        service_id=service_id,
        alias=alias,
    )

    project: Project = client.get_project(project_id=project_id)
    print(f" Project-id: {project.id} *** Project-name: {project.name}")

    dataset_data = {
        "name": dataset_name,
        "storage_url": "",
        "container_name": "",
        "sas_token": "",
        "data_source": DataSource.LOCAL,
        "data_folder": data_folder,  # local image folder
        "type": dataset_type,
        "generate_metadata": gen_metadata,
        "auto_tagging": AUTO_TAGGING_CLASSES if gen_auto_tagging else [],
        "annotation_format": annotation_format,
        "sequential": sequential,
        "reupload_dataset_uuid": reupload_dataset_uuid,
    }
    if dataset_type == DatasetType.ANNOTATED_DATA:
        dataset_data["annotations"] = ["groundtruth"]
    try:
        dataset = project.create_dataset(**dataset_data)
    except DataverseExceptionBase as e:
        logging.exception(f"Prepare dataset fail: {e.detail}")
        raise
    except Exception as e:
        logging.exception(f"Create dataset fail: {e}")
        raise

    print(f"Dataset: {dataset.name} is creating with id {dataset.id}")


def make_parser():
    parser = argparse.ArgumentParser("Resize Coco Dataset")
    parser.add_argument(
        "-host",
        "--host",
        required=True,
        type=str,
        help="the host url of the dataverse site (with curation port)",
        default=DataverseHost.STAGING,
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
        "-a",
        "--alias",
        type=str,
        default="default",
        help="the connection alias of your dataverse client",
    )
    parser.add_argument(
        "-project",
        "--project_id",
        type=str,
        help="The project id you want to import dataset",
    )
    parser.add_argument(
        "-folder",
        "--folder",
        type=str,
        help="the local data folder root folder for importing",
    )
    parser.add_argument(
        "-name", "--dataset_name", type=str, help="the dataset name for importing"
    )
    parser.add_argument(
        "-type",
        "--dataset_type",
        type=str,
        help="the dataset type (annotated_data / raw_data)",
    )
    parser.add_argument(
        "-anno",
        "--anno_format",
        type=str,
        help="the annotation_format for importing ex vision_ai / coco / image",
    )
    parser.add_argument(
        "--sequential",
        default=False,
        action="store_true",
        help="Whether dataset are sequential",
    )
    parser.add_argument(
        "--metadata",
        default=False,
        action="store_true",
        help="Whether generate metadata for your dataset",
    )
    parser.add_argument(
        "--auto_tagging",
        default=False,
        action="store_true",
        help="Whether generate auto_tagging for your dataset",
    )
    parser.add_argument(
        "-reupload",
        "--reupload_dataset_uuid",
        type=str,
        default=None,
        help=(
            "Dataset UUID of a previously failed local dataset import. "
            "If provided, the files that failed to upload (as recorded in `failed_upload.json`) "
            "will be re-uploaded."
        ),
    )

    return parser.parse_args()


if __name__ == "__main__":
    start = time.time()

    args = make_parser()
    import_dataset_from_local(
        host=args.host,
        email=args.email,
        password=args.password,
        service_id=args.service_id,
        alias=args.alias,
        project_id=args.project_id,
        data_folder=args.folder,
        dataset_name=args.dataset_name,
        dataset_type=args.dataset_type,
        annotation_format=args.anno_format,
        sequential=args.sequential,
        gen_metadata=args.metadata,
        gen_auto_tagging=args.auto_tagging,
        reupload_dataset_uuid=args.reupload_dataset_uuid,
    )

    end = time.time()
    logging.info("import_dataset_from_local complete, duration: {%d}s", end - start)

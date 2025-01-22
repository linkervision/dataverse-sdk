import argparse
import logging
import time

from dataverse_sdk import DataverseClient
from dataverse_sdk.constants import DataverseHost
from dataverse_sdk.exceptions.client import DataverseExceptionBase
from dataverse_sdk.schemas.client import Project
from dataverse_sdk.schemas.common import AnnotationFormat, DatasetType, DataSource


def import_vqa_dataset_from_local(
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
        "sensors": project.sensors,
        "data_source": DataSource.LOCAL,
        "data_folder": data_folder,  # local image folder
        "type": dataset_type,
        "generate_metadata": gen_metadata,
        "auto_tagging": gen_auto_tagging,
        "annotation_format": annotation_format,
        "sequential": sequential,
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
    parser = argparse.ArgumentParser("Import VQA Dataset")
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
        "-project",
        "--project_id",
        type=str,
        required=True,
        help="The project id you want to import dataset",
    )
    parser.add_argument(
        "-f",
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
        default="annotated_data",
        type=str,
        help="the dataset type (annotated_data / raw_data)",
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

    return parser.parse_args()


if __name__ == "__main__":
    args = make_parser()
    dataset_name = args.dataset_name
    if not dataset_name:
        dataset_name = time.strftime("%Y-%m-%d_%H-%M-%S")
    if args.dataset_type == DatasetType.ANNOTATED_DATA:
        annotation_format = AnnotationFormat.VLM
    else:
        annotation_format = AnnotationFormat.IMAGE
    import_vqa_dataset_from_local(
        host=args.host,
        email=args.email,
        password=args.password,
        service_id=args.service_id,
        project_id=args.project_id,
        data_folder=args.folder,
        dataset_name=dataset_name,
        dataset_type=args.dataset_type,
        annotation_format=annotation_format,
        sequential=args.sequential,
        gen_metadata=args.metadata,
        gen_auto_tagging=[],
    )

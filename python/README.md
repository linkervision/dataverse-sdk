# Dataverse SDK For Python
Dataverse is a MLOPs platform for assisting in data selection, data visualization and model training in computer vision.
Use Dataverse-SDK for Python to help you to interact with the Dataverse platform by Python. Currently, the library supports:
  - Create Project with your input ontology and sensors
  - Get Project by project-id
  - Create Dataset from your AWS storage or local
  - Get Dataset by dataset-id
  - Create Dataslice from a whole dataset, or look one up by name
  - List models for your selected project-id
  - Get and download your model
  - Convert a model to ONNX/TensorRT and read the convert record's metrics

[Package (PyPi)](https://pypi.org/project/dataverse-sdk/)    |   [Source code](https://github.com/linkervision/dataverse-sdk)


## Getting started

### Install the package

```
pip install dataverse-sdk
```

**Prerequisites**: You must have an Dataverse Platform Account and [Python 3.10+](https://www.python.org/downloads/) to use this package.

### Create the client

Interaction with the Dataverse site starts with an instance of the `DataverseClient` class. You need site url, an email-account and its password to instantiate the client object.

```Python
from dataverse_sdk import *
from dataverse_sdk.connections import get_connection
from dataverse_sdk.constants import DataverseHost

client = DataverseClient(
    host=DataverseHost.PRODUCTION.value, email="XXX", password="***", service_id="xxxx-xxxx-xx-xxx", alias="default", force = False
)
assert client is get_connection("default")

# Should provide different alias if you are trying to connect to different workspaces
client2 = DataverseClient(
    host=DataverseHost.PRODUCTION.value, email="account-2", password="***", service_id="xxxx-xxxx-xx-xxx", alias="client2", force = False
)
assert client2 is get_connection(client2.alias)

client3 = DataverseClient(
    host=DataverseHost.PRODUCTION.value, email="XXX", password="", service_id="xxxx-xxxx-xx-xxx", access_token="xxx", alias="client3", force = False
)
assert client3 is get_connection(client3.alias)
```

* Input arguments:

| Argument name      | Type/Options   | Default   | Description   |
| :---                 |     :---    |     :---  |          :--- |
| host        | str  | 	＊--    | the host url of the dataverse site |
| email  | str | ＊--  |  the email account of your dataverse workspace |
| password  | str | ＊--  |  the password of your dataverse workspace  |
| service_id  | str | ＊--   |  The service id of the dataverse you want to connect |
| alias | str | 'default' |  the connection alias of your dataverse client |
| force  | bool | False  |  whether force to replace the connection if the given alias exists |
| access_token  | str | None   | instead of password to do authentication |


## Key concepts

Once you've initialized a DataverseClient, you can interact with Dataverse from the initialized object.

## Examples

The following sections provide examples for the most common DataVerse tasks including:

* [Get User](#get-user)
* [List Projects](#list-projects)
* [Create Project](#create-project)
* [Get Project](#get-project)
* [Edit Project](#edit-project)
* [Update Alias](#update-ontology-alias)
* [Create Dataset](#create-dataset)
* [List Dataset](#list-and-get-dataset)
* [List Dataslices](#list-and-get-dataslices)
* [Get Dataslice by Name](#get-dataslice-by-name)
* [Create Dataslice from Dataset](#create-dataslice-from-dataset)
* [Export Dataslice](#export-dataslice-and-download)
* [List Models](#list-models)
* [Get and Download Model](#get-model)
* [Get Convert Model File](#get-convert-model-file)
* [Convert Model to ONNX/TRT](#convert-model-to-onnxtrt)
* [Retrieve Convert Record](#retrieve-convert-record-and-its-metrics)
* [Create VQA Project](#create-vqa-project)
* [Edit VQA Ontology](#edit-vqa-ontology)
* [Get Question List](#get-question-list)



### Get User

The `get_user` method is to list the current user info.
You can get the detail info, such as role, permission and user detail.

```python
user = client.get_user()
```

### List Projects
The `list_projects` method will list all projects of the given sites.

* Example Usage:
```Python
projects = client.list_projects(current_user = True,
                                exclude_sensor_type=SensorType.LIDAR,
                                image_type=OntologyImageType._2D_BOUNDING_BOX)

```

* Input arguments:

| Argument name      | Type/Options   | Default   | Description   |
| :---                 |     :---    |     :---  |          :--- |
| current_user         | bool  | True     | only show the projects of current user    |
| exclude_sensor_type  | SensorType.CAMERA <br>  SensorType.LIDAR| None  |   exclude the projects with the given sensor type  |
| image_type  | OntologyImageType._2D_BOUNDING_BOX <br> OntologyImageType.SEMANTIC_SEGMENTATION <br> OntologyImageType.INSTANCE_SEGMENTATION <br> OntologyImageType.CLASSIFICATION <br> OntologyImageType.POINT<br> OntologyImageType.POLYGON <br> OntologyImageType.POLYLINE <br> OntologyImageType.VQA | None  |  only include the projects with the given image type  |

<br>

### Create Project

The `create_project` method will create project on the connected site with the defined ontology and sensors.

* Example Usage:
```Python
# 1) Create ontology with ontologyclass object
ontology = Ontology(
    name="sample ontology",
    image_type=OntologyImageType._2D_BOUNDING_BOX,
    pcd_type = None,
    classes=[
        OntologyClass(name="Pedestrian", rank=1, color="#234567"),
        OntologyClass(name="Truck", rank=2, color="#345678"),
        OntologyClass(name="Car", rank=3, color="#456789"),
        OntologyClass(name="Cyclist", rank=4, color="#567890"),
        OntologyClass(name="DontCare", rank=5, color="#6789AB"),
        OntologyClass(name="Misc", rank=6, color="#789AB1"),
        OntologyClass(name="Van", rank=7, color="#89AB12"),
        OntologyClass(name="Tram", rank=8, color="#9AB123"),
        OntologyClass(name="Person_sitting", rank=9, color="#AB1234"),
    ],
)
```
For project with camera sensor, there would be only one image_type for one project. You could choose from `[OntologyImageType._2D_BOUNDING_BOX, OntologyImageType.SEMANTIC_SEGMENTATION, OntologyImageType.INSTANCE_SEGMENTATION, OntologyImageType.CLASSIFICATION, OntologyImageType.POINT, OntologyImageType.POLYGON, OntologyImageType.POLYLINE, OntologyImageType.VQA]`.

For project with lidar sensor, your should assign `pcd_type = OntologyPcdType.CUBOID` for the ontology.

```Python
# 2) Create your sensor list with name / SensorType
sensors = [
    Sensor(name="camera1", type=SensorType.CAMERA),
    Sensor(name="lidar1", type=SensorType.LIDAR),
]

# 3) Create your project tag attributes (Optional)
project_tag = ProjectTag(
    attributes=[
        {"name": "year", "type": "number"},
        {
            "name": "unknown_object",
            "type": "option",
            "options": [{"value": "fire"}, {"value": "leaves"}, {"value": "water"}],
        },
    ]
)

# 4) Create your project with your ontology/sensors/project_tag
project = client.create_project(name="Sample project", ontology=ontology, sensors=sensors, project_tag=project_tag)
```


* Input arguments for creating project:

| Argument name      | Type/Options   | Default   | Description   |
| :---                 |     :---    |     :---  |          :--- |
| name        | str  | ＊--    | name of your project    |
| ontology  | Ontology | ＊-- | the Ontology basemodel data of current project |
| sensors  | list[Sensor] | ＊-- |  the list of Sensor basemodel data of your project  |
| project_tag | ProjectTag | None |  your project tags  |
| description  | str | None | your project description  |

`＊--`: required argument without default

* Check https://linkervision.gitbook.io/dataverse/data-management/project-ontology for the detail of `Project Ontology`

<br>

### Get Project

The `get_project` method retrieves the project from the connected site. The `project_id` parameter is the unique integer ID of the project, not its "name" property.

```Python
project = client.get_project(project_id= 1, client_alias=client.alias) # if client_alias is not provided, we'll get it from client
```

<br>

### Edit Project

For editing project contents, we have four functions below for add/edit project tag and ontology classes.

#### Add New Project Tags
* Note: Can not create existing project tag!
```Python
tag = {
        "attributes": [
            {
                "name": "month",
                "type": "number"
            },
            {
                "name": "weather",
                "type": "option",
                "options": [{"value":"sunny"}, {"value":"rainy"}, {"value":"cloudy"}
                ]
            }]}
project_tag= ProjectTag(**tag)
#should provided client_alias if calling from client
client.add_project_tag(project_id = 10, project_tag=project_tag, client_alias=client.alias)
#OR
project.add_project_tag(project_tag=project_tag)
```

#### Edit Project Tags
** Note:
1. Can not edit project tag that does not exist
2. Can not modify the data type of existing project tags
3. Can not provide attributes with existing options

```Python
tag = {
        "attributes": [
            {
                "name": "weather",
                "type": "option",
                "options": [{"value":"unknown"}, {"value":"snowy"}
                ]
            }]}
project_tag= ProjectTag(**tag)
#should provided client_alias if calling from client
client.edit_project_tag(project_id = 10, project_tag=project_tag, client_alias=client.alias)
#OR
project.edit_project_tag(project_tag=project_tag)
```

#### Add New Ontology Classes

* Note: Can not add existing ontology class!
```Python
new_classes = [OntologyClass(name="obstruction",
                    rank=9,
                    color="#AB4321",
                    attributes=[{
                    "name":
                    "status",
                    "type":
                    "option",
                    "options": [{
                    "value": "static"}, {"value": "moving"
                    }]}])]
#should provided client_alias if calling from client
client.add_ontology_classes(project_id=24, ontology_classes=new_classes, client_alias=client.alias)
#OR
project.add_ontology_classes(ontology_classes=new_classes)
```


#### Edit Ontology Classes
** Note:
1. Can not edit ontology class that does not exist
2. Can not modify the data type of existing ontology class attributes
3. Can not provide attributes with existing options

```Python
edit_classes = [OntologyClass(name="obstruction",
                    color="#AB4321",
                    attributes=[{
                    "name":
                    "status",
                    "type":
                    "option",
                    "options": [{
                    "value": "unknown"}]}])]
#should provided client_alias if calling from client
client.edit_ontology_classes(project_id=24, ontology_classes=edit_classes, client_alias=client.alias)
#OR
project.edit_ontology_classes(ontology_classes=edit_classes)
```


### Update Ontology Alias

1. Get the csv file of alias map for your project
```Python
client.generate_alias_map(project_id=123, alias_file_path="./alias.csv")
```

2. Fill the alias in the csv file and save (DO NOT modify other fields)

3. Update alias for your project with the alias file path
```Python
client.update_alias(project_id=123, alias_file_path= "/Users/Downloads/alias.csv" )
```

<br>

### Create Dataset

**Required fields by `data_source`:**

| `data_source` | `storage_url` | `container_name` | `data_folder` | Notes |
| :--- | :---: | :---: | :---: | :--- |
| `DataSource.AWS` | ＊-- | - | ＊-- | use `access_key_id` + `secret_access_key` for a private S3 bucket |
| `DataSource.LOCAL` | - | - | ＊-- | local folder; SDK uploads files and sends `create_dataset_uuid` for you |
| `DataSource.SDK` | - | ＊-- | ＊-- | offline MinIO import: `container_name` = bucket, `data_folder` = path in bucket; needs Dataverse deployed in offline mode |

`＊--`: required for this `data_source` · `-`: not used (can be omitted)

> `DataSource.DATA_GENERATION` and `DataSource.PRE_IMPORT` require a `data_source_search_body` that the SDK does not currently send, so they are not supported via `create_dataset`.
>
> `DataSource.EXISTING_DATASETS` / `EXISTING_DATASLICES` only appear on datasets you read back from the API; they are not inputs for `create_dataset`.

#### Use `create_dataset` to import dataset from **cloud storage**

```Python
dataset_data = {
    "name": "Dataset 1",
    "data_source": DataSource.AWS,
    "storage_url": "storage/url",
    "data_folder": "datafolder/to/vai_anno",
    "type": DatasetType.ANNOTATED_DATA,
    "annotation_format": AnnotationFormat.VISION_AI,
    "annotations": ["groundtruth"],
    "sequential": False,
    "render_pcd": False,
    "generate_metadata": False,
    "access_key_id": "aws s3 access key id",  # only for private s3 bucket, don't need to assign it in case of public s3 bucket
    "secret_access_key": "aws s3 secret access key",  # only for private s3 bucket, don't need to assign it in case of public s3 bucket
}
dataset = project.create_dataset(**dataset_data)

```

* Input arguments for creating dataset from `cloud storage`:

| Argument name      | Type/Options   | Default | Description   |
| :---                 |     :---    |     :---  |          :--- |
| name        | str  | ＊--    | name of your dataset    |
| data_source | DataSource.AWS | ＊-- | the datasource of your dataset |
| storage_url | str | ＊-- |  your cloud storage url  |
| data_folder | str | ＊-- |  the relative data folder from the storage_url  |
| type | DatasetType.ANNOTATED_DATA <br> DatasetType.RAW_DATA | ＊-- |  your dataset type  (annotated or raw data)|
| annotation_format | AnnotationFormat.VISION_AI <br> AnnotationFormat.KITTI <br> AnnotationFormat.COCO <br> AnnotationFormat.YOLO <br> AnnotationFormat.IMAGE <br> AnnotationFormat.BDDP <br> AnnotationFormat.VIDEO <br> AnnotationFormat.VLM <br>| ＊-- |  the format of your annotation data  |
| annotations | list[str] | None |  list of names for your annotation data folders, such as ["groundtruth"]  |
| sequential | bool | False | data is sequential or not   |
| render_pcd | bool | False | render pcd preview image or not |
| generate_metadata | bool | False | generate image meta data or not   |
| description  | str | None | your dataset description  |
| access_key_id | str | None |  access key id for AWS private s3 bucket  |
| secret_access_key | str | None| secret access key for AWS private s3 bucket  |

`＊--`: required for cloud storage

* Check https://linkervision.gitbook.io/dataverse/data-management/import-dataset for the detail of `Import Dataset`.

<br>

#### Use `create_dataset` to import dataset from `LOCAL`

```Python
dataset_data2 = {
    "name": "dataset-local-upload",
    "data_source": DataSource.LOCAL,
    "data_folder": "/YOUR/TARGET/LOCAL/FOLDER",
    "type": DatasetType.ANNOTATED_DATA, # or DatasetType.RAW_DATA for images
    "annotation_format": AnnotationFormat.VISION_AI,
    "annotations": ["groundtruth"],  # remove it when type is DatasetType.RAW_DATA
    "sequential": False,
    "generate_metadata": False,
}
dataset2 = project.create_dataset(**dataset_data2)

```

Your could also use the script for importing dataset from local
```
python tools/import_dataset_from_local.py -host {YOUR_HOST} -e {your-account-email} -p {PASSWORD} -s {service-id}  -project {project-id} --folder {/YOUR/TARGET/LOCAL/FOLDER} -name {dataset-name} -type {raw_data OR annotated_data} -anno {image OR vision_ai} --sequential
```
<br>

### List and Get Dataset

The `list_datasets` method would return the list of dataset under the given project
```Python
project = client.get_project(project_id=1)
datasets:list = project.list_datasets()
```
OR
```Python
datasets:list = client.list_datasets(project_id=1, client_alias=client.alias )
```


The `get_dataset` method retrieves the dataset info from the connected site. The `dataset_id` parameter is the unique integer ID of the dataset, not its "name" property.

```Python
dataset = client.get_dataset(dataset_id=5)
```
<br>


### List and Get Dataslices
```Python
# list dataslices with project_id
client.list_dataslices(project_id=101, client_alias=client.alias)

# Get target dataslice data
dataslice_data = client.get_dataslice(dataslice_id=504)

```

### Get Dataslice by Name

Dataslice names are unique across the site, so a name identifies exactly one dataslice.

```Python
dataslice = client.get_dataslice_by_name(project_id=101, dataslice_name="my-dataslice")
# OR from the project object
project = client.get_project(project_id=101)
dataslice = project.get_dataslice_by_name(dataslice_name="my-dataslice")

dataslice.id
dataslice.status      # DataSliceStatus
dataslice.image_count
```

`DataSliceStatus`: `CREATING`, `CREATING_FAIL`, `READY`, `ANNOTATION_UPDATING`,
`IQA_UPDATING`, `TAGGING_UPDATING`, `DELETING`.

`image_count` and `pcd_count` read the per-type datarow counts in `dataslice.metadata`.
Which of them carries a number follows the slice's own `type`:

| `dataslice.type`     | `image_count` | `pcd_count` |
| -------------------- | ------------- | ----------- |
| `image`              | images        | `None`      |
| `pcd`                | `None`        | pcds        |
| `frame` / `sequence` | images        | pcds        |

The last row is the fused camera-plus-lidar case, where both are counted and both can be
non-zero. `None` means "this slice holds no datarow of that type", never zero.

Only `get_dataslice` and `get_dataslice_by_name` return `metadata`; a dataslice from
`list_dataslices` has none, so **both counts are `None` there**. Such a listing carries
`file_count` instead — the slice's image and pcd datarows counted together.

This is the only method that takes a name: everything downstream — `convert_model`
included — takes ids, so this is how a name becomes one.

Raises `ValueError` when the project holds no dataslice of that name.

### Create Dataslice from Dataset

`create_dataslice_from_dataset` turns a whole dataset into a dataslice — the equivalent of
opening it in Data Visualization and hitting **Save Data Slice** without narrowing the
selection. It is **asynchronous**: the returned dataslice starts out `creating` with no
file count, and turns `ready` once the datarows are in.

```Python
dataslice = client.create_dataslice_from_dataset(
    project_id=101,
    dataset_id=5,
    dataslice_name="whole-dataset-slice",
)
# OR from the project object
dataslice = project.create_dataslice_from_dataset(
    dataset_id=5, dataslice_name="whole-dataset-slice"
)

# Poll until it settles
import time

from dataverse_sdk import DataSliceStatus

SETTLED = (DataSliceStatus.READY, DataSliceStatus.CREATING_FAIL)
while dataslice.status not in SETTLED:
    time.sleep(10)
    dataslice = client.get_dataslice(dataslice_id=dataslice.id)
print(dataslice.status, dataslice.image_count)  # "ready" 11
```

A convert record polls the same way through `get_convert_record`, with
`ConvertRecordStatus.READY` and `ConvertRecordStatus.FAILED` as its settled states.

| Argument name  | Type | Default | Description                                        |
| -------------- | ---- | ------- | -------------------------------------------------- |
| project_id     | int  | ＊--    | Project the dataset belongs to                     |
| dataset_id     | int  | ＊--    | Every datarow of this dataset goes into the slice  |
| dataslice_name | str  | ＊--    | Must be unused **site-wide**                       |
| description    | str  | None    |                                                    |

`＊--`: required argument without default

### Export Dataslice and Download
```Python
# Trigger export and get export record id
export_record = client.export_dataslice(dataslice_id=504)
# Use export record id to download export data
client.download_export_dataslice_data(dataslice_id=504, export_record_id=export_record["export_record_id"])

```


### List Models

The `list_models` method will list all the models in the given project. You can filter models by type using the `type` parameter.

#### Basic Usage

```Python
# Method 1: Using client
models = client.list_models(project_id=1, client_alias=client.alias)

# Method 2: Using project object
project = client.get_project(project_id=1)
models = project.list_models()
```

Each model carries a `status`, which is what a listing is usually narrowed by:

```Python
from dataverse_sdk import MLModelStatus

ready = [model for model in models if model.status == MLModelStatus.READY]
```

`MLModelStatus`: `PROCESSING`, `READY`, `DELETING`.

#### Filtering by Model Type

You can filter models by type using strings or lists of strings. The SDK supports multiple model types:

```Python
# Filter by single type using string
models = client.list_models(project_id=1, type="trained", client_alias=client.alias)

# Filter by single type using list
models = client.list_models(project_id=1, type=["trained"], client_alias=client.alias)

# Filter by multiple types using list
models = client.list_models(
    project_id=1,
    type=["trained", "byom", "uploaded"],
    client_alias=client.alias
)
```

#### Available Model Types

| String Value | Description          |
| ------------ | -------------------- |
| `"trained"`  | Trained models       |
| `"byom"`     | Bring Your Own Model |
| `"uploaded"` | Uploaded models      |

#### Input Arguments

| Argument name | Type/Options                                                      | Default             | Description              |
| ------------- | ----------------------------------------------------------------- | ------------------- | ------------------------ |
| project_id    | int                                                               | ＊--                | The project ID           |
| client_alias  | str                                                               | None                | The client alias         |
| type          | "trained", "byom", "uploaded", list["trained", "byom", "uploaded] | ["trained", "byom"] | Model types to filter by |

`＊--`: required argument without default

<br>

### Get Model
The `get_model` method will get the model detail info by the given model-id

```Python
model = client.get_model(model_id=30, client_alias=client.alias)
model = project.get_model(model_id=30)
```
From the given model, we could get the model convert records as below
```Python
model_record = client.get_convert_record(convert_record_id=1, client_alias=client.alias)
OR
model_record = model.get_convert_record(convert_record_id=1)
```
<br>

* If the converted model format is onnx, you could download the model as below.
```Python
# Get the target convert record, and download labels.txt and model.onnx
model_record = model.get_convert_record(convert_record_id=5)
status, label_file_path = model_record.get_label_file(save_path="./labels.txt", timeout=6000)
status, onnx_model_path = model_record.get_onnx_model_file(save_path="./model.onnx", timeout=6000)
```

### Get Convert Model File

The `get_convert_model_file` method will download one of the stored artifacts of a convert record, selected by the `file_type` argument.

```Python
from dataverse_sdk import ConvertModelFileType

# Method 1: Using client
status, save_path = client.get_convert_model_file(
    convert_record_id=5,
    file_type=ConvertModelFileType.TRITON,
    client_alias=client.alias,
)

# Method 2: Using convert record object
model_record = model.get_convert_record(convert_record_id=5)
status, save_path = model_record.get_convert_model_file(file_type=ConvertModelFileType.TRITON)
```

#### Available File Types

| Enum Value                          | String Value      | File downloaded                                                        | Available when         | Default `save_path`       |
| ----------------------------------- | ----------------- | ---------------------------------------------------------------------- | ---------------------- | ------------------------- |
| `ConvertModelFileType.TRITON`       | `"triton"`        | Triton bundle (`.zip`)                                                 | always                 | `./triton.zip`            |
| `ConvertModelFileType.MODEL`        | `"model"`         | Main artifact: `model.onnx` (format=onnx) or `trt.engine` (format=trt)  | always                 | `./converted_model`       |
| `ConvertModelFileType.RAW_ONNX`     | `"raw_onnx"`      | Intermediate onnx (fp16: `trt.onnx`; D-FINE int8: fp32 `model.onnx`)    | format=trt             | `./raw.onnx`              |
| `ConvertModelFileType.CALIB_CACHE`  | `"calib_cache"`   | `trt_int8_calib.cache` (int8 scale table)                              | D-FINE + int8 only     | `./trt_int8_calib.cache`  |

When `save_path` is omitted, the filename is resolved in this order:

1. The name the server supplies in the response's `Content-Disposition` header — this is the artifact's real stored filename (e.g. `ptq.engine`, `triton_model.zip`), so it already reflects the convert format and precision.
2. The **Default `save_path`** in the table above, used only when the server sends no usable filename.

Pass an explicit `save_path` whenever you need a predictable location — it always takes precedence over the server's name.

#### Input Arguments

| Argument name     | Type/Options                                                   | Default                       | Description                                                          |
| ----------------- | -------------------------------------------------------------- | ----------------------------- | -------------------------------------------------------------------- |
| convert_record_id | int                                                            | ＊--                          | The convert record to download from                                  |
| file_type         | ConvertModelFileType, "triton", "model", "raw_onnx", "calib_cache" | ConvertModelFileType.TRITON | Which artifact to download                                           |
| save_path         | str                                                            | None                          | Local path to write the file; when omitted the server's filename is used (see above) |
| timeout           | int                                                            | 3000                          | Maximum timeout of the request                                        |
| permission        | str                                                            | ""                            | Sets the `X-Request-Source` header; pass the caller's permission source if required |
| client            | DataverseClient                                                | None                          | Client instance; if omitted, `client_alias` must be given             |
| client_alias      | str                                                            | None                          | Registered client alias; required when `client` is None               |

`＊--`: required argument without default

#### Return

`tuple[bool, str]` — `(status, save_path)`. `status` is `False` if the download failed, and `save_path` is the path that was written to.

<br>


### Convert Model to ONNX/TRT

`convert_model` starts a model conversion. It is **asynchronous**: the call returns the
ids of the created convert records, and the conversion itself finishes later.

```Python
from dataverse_sdk import ConvertFormat, ConvertPrecision, QuantizationMethod

# Method 1: Using client -- model 6418 is a YOLOv9, which is NMS-based
record_ids = client.convert_model(
    model_id=6418,
    name="onnx-fp16",
    target_dataslice_id=504,                # ids only; see Get Dataslice by Name
    model_type=ConvertFormat.ONNX,
    data_type=ConvertPrecision.FP16,
    confidence_score=10,
    nms_threshold=50,
    client_alias=client.alias,
)

# Method 2: Using the model object -- model 7201 is a D-FINE, which is NMS-free
model = client.get_model(model_id=7201, client_alias=client.alias)
record_ids = model.convert(
    name="trt-int8-ptq",
    target_dataslice_id=504,
    model_type=ConvertFormat.TRT,
    data_type=ConvertPrecision.INT8,
    quantizations=[QuantizationMethod.PTQ],
    quantize_dataslice_id=508,
)
```

#### Input Arguments

| Argument name      | Type/Options                          | Default   | Description                                                                     |
| ------------------ | ------------------------------------- | --------- | ------------------------------------------------------------------------------- |
| model_id           | int                                   | ＊--      | The source model to convert                                                     |
| name               | str                                   | ＊--      | Convert record name; must be unused **under this model**                        |
| target_dataslice_id| int                                   | ＊--      | Dataslice the converted model is evaluated on                                   |
| model_type         | ConvertFormat \| str                  | ＊--      | Target format                                                                   |
| data_type          | ConvertPrecision \| str               | ＊--      | Numeric precision; `fp32` is D-FINE only                                        |
| confidence_score   | int                                   | 10        | Confidence threshold, 10-90                                                     |
| iou                | int                                   | 50        | IoU threshold, 1-99                                                             |
| topk               | int                                   | None      | 50-300; 300 for D-FINE, 100 for every other structure                           |
| main_obj_low       | int                                   | 1024      | Main object size lower bound                                                    |
| main_obj_high      | int                                   | 9216      | Main object size upper bound                                                    |
| resolution_width   | int                                   | None      | Defaults to the source model's own resolution                                   |
| resolution_height  | int                                   | None      | Defaults to the source model's own resolution                                   |
| nms_threshold      | int                                   | None      | 10-90; required for NMS-based architectures, **rejected for D-FINE**            |
| nms_class_agnostic | bool                                  | None      | **Rejected for D-FINE**                                                         |
| machine_type       | str                                   | None      |                                                                                 |
| quantize_dataslice_id | int                                | None      | Calibration dataslice; required when `quantizations` is given                   |
| quantizations      | list[QuantizationMethod \| str]       | None      | Exactly one method; only `PTQ` is supported for D-FINE                          |
| model_structure    | ModelStructure \| str                 | None      | The source model's architecture; sets the topk default and the D-FINE rules. Read back from the model when omitted |

`＊--`: required argument without default

Every enum below is a `str` enum, so the member and its plain string are interchangeable:

| Enum                 | Members                                          | Strings                                                                                     |
| -------------------- | ------------------------------------------------ | ------------------------------------------------------------------------------------------- |
| `ConvertFormat`      | `ONNX`, `TRT`                                    | `"onnx"`, `"trt"`                                                                           |
| `ConvertPrecision`   | `FP32`, `FP16`, `INT8`                           | `"fp32"`, `"fp16"`, `"int8"`                                                                |
| `QuantizationMethod` | `PTQ`, `QAT_TRAIN`, `QAT_DISTILL`                | `"ptq"`, `"qat_train"`, `"qat_distill"`                                                     |
| `ModelStructure`     | `YOLOV9_C`, `YOLOV9_E`, `YOLOV9_S`               | `"yolov9-c"`, `"yolov9-e"`, `"yolov9-s"`                                                    |
|                      | `DFINE_N`, `DFINE_S`, `DFINE_M`, `DFINE_L`, `DFINE_X` | `"dfine-n"`, `"dfine-s"`, `"dfine-m"`, `"dfine-l"`, `"dfine-x"`                        |

`convert_model`, `get_convert_record`, `get_convert_record_by_name` and
`list_convert_records` are static methods, so they need `client_alias` (or `client`)
passed in; without either they raise `ValueError`. The dataslice methods above are
instance methods and fall back to the client's own alias.

Supported resolutions are `640x480`, `640x640`, `1024x576`, `1024x768` and `1024x1024`; an unsupported one raises `APIValidationError` without sending the request.

The rest of the rules depend on the source model's architecture:

| `model_structure` | `data_type` | `model_type`    | `nms_threshold` | `quantizations`                   |
| ----------------- | ----------- | --------------- | --------------- | --------------------------------- |
| yolov9            | `fp16`      | `onnx` or `trt` | required        | none                              |
| yolov9            | `int8`      | `trt`           | required        | `ptq`, `qat_train`, `qat_distill` |
| D-FINE            | `fp32`      | `onnx`          | rejected        | none                              |
| D-FINE            | `fp16`      | `trt`           | rejected        | none                              |
| D-FINE            | `int8`      | `trt`           | rejected        | `ptq`                             |

`nms_class_agnostic` follows `nms_threshold`. Every rule here is checked before the request is sent, except that yolov9's `int8` is limited to `trt` server-side.

#### Return

`list[int]` — ids of the created convert records, one per quantization method. The
backend takes only one quantization method for now, so today the list always holds
exactly one id.

#### Keeping it to one request

Both dataslices are given by id, so the only extra request left is the read-back of the
source model — and passing `model_structure` **together with both resolutions** skips
that too, which is what `model.convert()` does with the three it already holds. Chaining
from objects you already have is therefore the cheapest form:

```Python
model = client.get_model(model_id=6418, client_alias=client.alias)
dataslice = project.get_dataslice_by_name("my-eval-dataslice")

record_ids = model.convert(
    name="onnx-fp16",
    target_dataslice_id=dataslice.id,
    model_type=ConvertFormat.ONNX,
    data_type=ConvertPrecision.FP16,
    nms_threshold=50,
)

record = client.get_convert_record(convert_record_id=record_ids[0], client_alias=client.alias)
record.status
```

<br>


### Retrieve Convert Record and its Metrics

A convert name is only unique **per source model** — two models may each own a record
called `onnx-fp16` — so a lookup by name takes the model alongside it.

```Python
# By id
record = client.get_convert_record(convert_record_id=2212, client_alias=client.alias)

# By name, under a given model
record = client.get_convert_record_by_name(
    model_id=6418, convert_name="onnx-fp16", client_alias=client.alias
)
# OR from the model object
model = client.get_model(model_id=6418, client_alias=client.alias)
record = model.get_convert_record_by_name(convert_name="onnx-fp16")

record.status      # ConvertRecordStatus
record.id
record.model_type  # "onnx" / "trt"      -- reads configuration["format"]
record.data_type   # "fp32"/"fp16"/"int8" -- reads configuration["precision"]
record.f1_score    # and .precision, .recall, .map_5_95
```

`record.configuration` carries every other convert setting. The four metrics read as
`0.0` until the conversion finishes, and `None` when the server did not return them at all
— mind that `record.precision` is one of them, and the precision the model was converted
at is `record.data_type`.

Looking a record up by name, filtering a listing, and reading the ids back from
`convert_model` all need a Dataverse recent enough to serialise them; against an older
site the SDK falls back to filtering the full listing itself.

#### List Convert Records

```Python
# Every convert record in a project
records = client.list_convert_records(project_id=1992, client_alias=client.alias)
# OR
records = project.list_convert_records()

# Narrow by model / name / status
records = client.list_convert_records(
    model_id=6418, status="ready", client_alias=client.alias
)
# OR
records = model.list_convert_records(status="ready")
```

<br>


### Create VQA Project

The `create_vqa_project` method will create project on the connected site with the defined questions/answer_type.

* Example Usage:
```Python
# 1) Create question class with question and answer type pair
question_answer = [ QuestionClass(class_name="question1", rank=1, question="Is any person found in the picture?",
                    answer_type="boolean"),
                    QuestionClass(class_name="question2", rank=2, question="What is the blob color of traffic light?",   answer_type="option",answer_options=["red","yellow","green"])
                   ]
```

```Python
# 2) Create your VQA project as below
project = client.create_vqa_project(name="vqa-project", sensor_name="camera1", ontology_name="vqa-ontology", question_answer=question_answer)
```

* Input arguments for creating project:

| Argument name      | Type/Options   | Default   | Description   |
| :---                 |     :---    |     :---  |          :--- |
| name        | str  | ＊--    | name of your project    |
| sensor_name | str | ＊-- |  the camera sensor name  |
| ontology_name | str |  *--  |  the ontology name |
| question_answer|  list[QuestionClass] |  *--  |  your question/answer_type. `QuestionClass.answer_type` valid values: `boolean`, `option`, `number`, `text`  |
| description  | str | None | your project description  |

`＊--`: required argument without default


<br>


### Edit VQA Ontology
** Note:
1. Can not edit question answer type
2. Can not update with existing answer options
3. Can not add question with existing rank id

```Python
create_questions = [QuestionClass(class_name="question3", rank=3, question="Age?",answer_type="number")]
update_questions = [{"rank": 2, "question": "What is the blob color of traffic light?(the closet one)", "options":["black"] }]

# Through client
client.edit_vqa_ontology(project_id=24, ontology_name="ontology-new-name",
                         create=create_questions,
                         update=update_questions,
                         client_alias=client.alias)

# Through project object
project.edit_vqa_ontology(ontology_name="ontology-new-name",
                          create=create_questions,
                          update=update_questions)
```

### Get Question List

The function below could help you get the question list of VQA project
(which could help you to prepare the annotated data)
```Python
output = client.get_question_list(project_id=107, output_file_path="./question.json" )

```



## Quick Tools

### Import Your Local Dataset
```
python tools/import_dataset_from_local.py -host {YOUR_HOST} -e {your-account-email} -p {PASSWORD} -s {service-id}  -project {project-id} --folder {/YOUR/TARGET/LOCAL/FOLDER} -name {dataset-name} -type {raw_data OR annotated_data} -anno {image OR vision_ai} --sequential
```

### Import VQA Local Dataset
```
python tools/import_vqa_dataset.py -host {YOUR_HOST} -e {your-account-email} -p {PASSWORD} -s {service-id} -project {project-id} --folder {/YOUR/TARGET/LOCAL/FOLDER} -type {raw_data OR annotated_data}

```

### Export Dataslice and download files
```
python tools/export_dataslice.py -host {YOUR_HOST}  -e {your-account-email} -p {PASSWORD} -s {service-id} -dataslice {dataslice_id} -f {/YOUR/TARGET/LOCAL/file.zip}
```

### Export Large Dataslice and download files
```
python tools/export_dataslice_large.py -host {YOUR_HOST} -e {your-account-email} -p {PASSWORD} -s {service-id} -dataslice {dataslice_id} --anno {export-model-name / groundtruth} --target_folder {folder path} --export-format {coco, visionai, yolo, vlm ...etc}
```

### Upload videos to create session tasks
```
python tools/upload_videos_create_session.py -host {YOUR_HOST} -e {your-account-email} -p {PASSWORD} -s {service-id} -f {/YOUR/VIDEOS/LOCAL/FOLDER} -n {session-name}
```

- Advanced arguments for video curation (sequential data):

| Argument name              | Type/Options   | Default   | Description                                                                 |
|----------------------------|----------------|-----------|-----------------------------------------------------------------------------|
| --video-curation            | bool | False     | enable video curation (sequential data)                                                     |
| --global-mean-threshold     | float          | 0.001     | Threshold for the video's global average motion magnitude (0.000001 ~ 0.01). Higher values are stricter (flag more clips as low-motion); lower values are looser (flag fewer clips). |
| --per-patch-256-min-threshold | float        | 0.000001  | Minimum average motion magnitude allowed in any 256x256 pixel patch (0.000001 ~ 0.0001). Higher values are stricter per-patch (flag more clips when any 256x256 patch is too still); lower values are looser (flag fewer clips). |
| --split-duration            | int            | 5         | Set the length of each split clip in seconds (2 ~ 30s).                     |

## Links to language repos

[Python Readme](https://github.com/linkervision/dataverse-sdk/blob/master/python/README.md)

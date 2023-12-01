# Dataverse SDK For Python
Dataverse is a MLOPs platform for assisting in data selection, data visualization and model training in comupter vision.
Use Dataverse-SDK for Python to help you to interact with the Dataverse platform by Python. Currently, the library supports:
  - Create Project with your input ontology and sensors
  - Get Project by project-id
  - Create Dataset from your AWS/Azure storage or local
  - Get Dataset by dataset-id
  - List models for your selected project-id
  - Get and download your model

[Package (PyPi)](https://pypi.org/project/dataverse-sdk/)    |   [Source code](https://github.com/linkernetworks/dataverse-sdk)


## Getting started

### Install the package

```
pip install dataverse-sdk
```

**Prerequisites**: You must have an Dataverse Platform Account and [Python 3.9+](https://www.python.org/downloads/) to use this package.

### Create the client

Interaction with the Dataverse site starts with an instance of the `DataverseClient` class. You need site url, an email-account and its password to instantiate the client object.

```Python
from dataverse_sdk import *
from dataverse_sdk.connections import get_connection
client = DataverseClient(
    host=DataverseHost.PRODUCTION, email="XXX", password="***"
)
assert client is get_connection()
```


## Key concepts

Once you've initialized a DataverseClient, you can interact with Dataverse from the initialized object.

## Examples

The following sections provide examples for the most common DataVerse tasksm including:

* [List Projects](#list-projects)
* [Create Project](#create-project)
* [Get Project](#get-project)
* [Create Dataset](#create-dataset)
* [Get Dataset](#get-dataset)
* [List Models](#list-models)
* [Get and Download Model](#get-model)

### List Projects
The `list_projects` method will list all projects of the given sites.

* Example Usage:
```Python
projects = client.list_projects(current_user = True,
                                exclude_sensor_type=SensorType.LIDAR,
                                image_type= OntologyImageType._2D_BOUNDING_BOX)

```

* Input arguments:

| Argument name      | Type/Options   | Default   | Description   |
| :---                 |     :---    |     :---  |          :--- |
| current_user         | bool  | True     | only show the projects of current user    |
| exclude_sensor_type  | SensorType.CAMERA <br>  SensorType.LIDAR| None  |   exclude the projects with the given sensor type  |
| image_type  | OntologyImageType._2D_BOUNDING_BOX <br> OntologyImageType.SEMANTIC_SEGMENTATION <br> OntologyImageType.CLASSIFICATION <br> OntologyImageType.POINT<br> OntologyImageType.POLYGON <br> OntologyImageType.POLYLINE | None  |  only include the projects with the given image type  |

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
For project with camera sensor, there would be only one image_type for one project. You could choose from `[OntologyImageType._2D_BOUNDING_BOX, OntologyImageType.SEMANTIC_SEGMENTATION, OntologyImageType.CLASSIFICATION, OntologyImageType.POINT, OntologyImageType.POLYGON, OntologyImageType.POLYLINE]`.

For project with lidar sensor, your should assign `pcd_type = OntologyPcdType.CUBOID` for the ontology.

```Python
# 2) Create your sensor list with name / SensorType
sensors = [
    Sensor(name="camera_1", type=SensorType.CAMERA),
    Sensor(name="lidar_1", type=SensorType.LIDAR),
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
| name        | str  | *--    | name of your project    |
| ontology  | Ontology | *-- | the Ontology basemodel data of current project |
| sensors  | list[Sensor] | *-- |  the list of Sensor basemodel data of your project  |
| project_tag | ProjectTag | None |  your project tags  |
| description  | str | None | your project description  |

`＊--`: required argument without default

* Check https://linkervision.gitbook.io/dataverse/data-management/project-ontology for the detail of `Project Ontology`

<br>

### Get Project

The `get_proejct` method retrieves the project from the connected site. The `project_id` parameter is the unique interger ID of the project, not its "name" property.

```Python
project = client.get_project(project_id= 1)
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
client.add_project_tag(project_id = 10, project_tag=project_tag)
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
client.edit_project_tag(project_id = 10, project_tag=project_tag)
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
client.add_ontology_classes(project_id=24, ontology_classes=new_classes)
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
client.edit_ontology_classes(project_id=24, ontology_classes=edit_classes)
#OR
project.edit_ontology_classes(ontology_classes=edit_classes)
```

<br>

### Create Dataset

#### Use `create_dataset` to import dataset from **cloud storage**

```Python
dataset_data = {
    "name": "Dataset 1",
    "data_source": DataSource.Azure/DataSource.AWS,
    "storage_url": "storage/url",
    "container_name": "azure container name",
    "data_folder": "datafolder/to/vai_anno",
    "sensors": project.sensors,
    "type": DatasetType.ANNOTATED_DATA,
    "annotation_format": AnnotationFormat.VISION_AI,
    "annotations": ["groundtruth"],
    "sequential": False,
    "render_pcd": False,
    "generate_metadata": False,
    "auto_tagging": ["timeofday"],
    "sas_token": "azure sas token",  # only for azure storage
    "access_key_id" : "aws s3 access key id",# only for private s3 bucket, don't need to assign it in case of public s3 bucket or azure data source
    "secret_access_key": "aws s3 secret access key"# only for private s3 bucket, don't need to assign it in case of public s3 bucket or azure data source
}
dataset = project.create_dataset(**dataset_data)
```

* Input arguments for creating dataset from cloud storage:

| Argument name      | Type/Options   | Default | Description   |
| :---                 |     :---    |     :---  |          :--- |
| name        | str  | ＊--    | name of your dataset    |
| data_source | DataSource.Azure <br> DataSource.AWS | ＊-- | the datasource of your dataset |
| storage_url | str | ＊-- |  your cloud storage url  |
| container_name | str | None |  azure container name  |
| data_folder | str | ＊-- |  the relative data folder from the storage_url and container  |
| sensors  | list[Sensor] | ＊-- |  the list of Sensor of your dataset (one or more from project specified sensors)  |
| type | DatasetType.ANNOTATED_DATA <br> DatasetType.RAW_DATA | ＊-- |  your dataset type  (annotated or raw data)|
| annotation_format | AnnotationFormat.VISION_AI <br> AnnotationFormat.KITTI <br> AnnotationFormat.COCO <br> AnnotationFormat.IMAGE | ＊-- |  the format of your annotation data  |
| annotations | list[str] | None |  list of names for your annotation data folders, such as ["groundtruth"]  |
| sequential | bool | False | data is sequential or not   |
| render_pcd | bool | False | render pcd preview image or not |
| generate_metadata | bool | False | generate image meta data or not   |
| auto_tagging | list | None | generate auto_tagging with target models `["weather", "scene", "timeofday"]`   |
| description  | str | None | your dataset description  |
| sas_token | str | None | SAStoken for azure container  |
| access_key_id | str | None |  access key id for AWS private s3 bucket  |
| secret_access_key | str | None| secret access key for AWS private s3 bucket  |

`＊--`: required argument without default

* Check https://linkervision.gitbook.io/dataverse/data-management/import-dataset for the detail of `Import Dataset`.

<br>



### Get Dataset

The `get_dataset` method retrieves the dataset info from the connected site. The `dataset_id` parameter is the unique interger ID of the dataset, not its "name" property.

```Python
dataset = client.get_dataset(dataset_id=5)
```
<br>

### List Models
The `list_models` method will list all the models in the given project

```Python
#1
models = client.list_models(project_id = 1)
#2
project = client.get_project(project_id=1)
models = project.list_models()
```
<br>

### Get Model
The `get_model` method will get the model detail info by the given model-id

```Python
model = client.get_model(model_id=30)
model = project.get_model(model_id=30)
```
From the given model, we could get the label file / triton model file / onnx model file by the commands below.
```Python
status, label_file_path = model.get_label_file(save_path="./labels.txt", timeout=6000)
status, triton_model_path = model.get_triton_model_file(save_path="./model.zip", timeout=6000)
status, onnx_model_path = model.get_onnx_model_file(save_path="./model.onnx", timeout=6000)
```
<br>

## Troubleshooting


## Next steps


## Contributing



## Links to language repos

[Python Readme](https://github.com/linkernetworks/dataverse-sdk/tree/develop/python/README.md)

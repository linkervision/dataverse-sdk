# Dataverse SDK For Python
Dataverse is a MLOPs platform for assisting in data selection, data visualization and model training in comupter vision.
Use Dataverse-SDK for Python to help you to interact with the Dataverse platform by Python. Currently, the library supports:
  - Create Project with your input ontology and sensors
  - Get Project by project-id
  - Create Dataset from your AWS/Azure storage or local
  - Get Dataset by dataset-id

[Package (PyPi)](https://test.pypi.org/project/dataverse-sdk/)    |   [Source code](https://github.com/linkernetworks/dataverse-sdk)


## Getting started

### Install the package

```
pip install dataverse-sdk
```

**Prerequisites**: You must have an Dataverse Platform Account and [Python 3.9+](https://www.python.org/downloads/) to use this package.

### Create the client

Interaction with the Dataverse site starts with an instance of the `DataverseClient` class. You need an email-account and its password to instantiate the client object.

```Python
from dataverse_sdk import *
from dataverse_sdk.connections import get_connection
client = DataverseClient(
    host=DataverseHost.STAGING, email="XXX", password="***"
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

### List Projects
The `list_projects` method will list all projects of the given sites.


```Python
projects = client.list_projects(current_user = True,
                                exclude_sensor_type=SensorType.LIDAR,
                                image_type= OntologyImageType._2D_BOUNDING_BOX)

```
### Create Project

The `create_project` method will create project on the connected site with the defined ontology and sensors.

```Python
ontology = Ontology(
    name="test ot",
    image_type=OntologyImageType._2D_BOUNDING_BOX,
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
sensors = [
    Sensor(name="camera 1", type=SensorType.CAMERA),
    Sensor(name="lidar 1", type=SensorType.LIDAR),
]
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

project = client.create_project(name="test project", ontology=ontology, sensors=sensors, project_tag=project_tag)
```

### Get Project

The `get_proejct` method retrieves the project from the connected site. The `id` parameter is the unique interger ID of the project, not its "name" property.

```Python
project = client.get_project(id)
```

### Create Dataset

* Use `create_dataset` to create dataset from **cloud storage**

```Python
dataset_data = {
    "data_source": DataSource.Azure/Datasource.AWS,
    "storage_url": "storage/url",
    "container_name": "azure container name",
    "data_folder": "datafolder/to/vai_anno",
    "sas_token": "azure sas token",
    "name": "Dataset 1",
    "type": DatasetType.ANNOTATED_DATA,
    "annotations": ["groundtruth"]
    "generate_metadata": False,
    "render_pcd": False,
    "annotation_format": AnnotationFormat.VISION_AI,
    "sequential": False,
    "sensors": project.sensors,
}
dataset = project.create_dataset(**dataset_data)
```

* Use `create_dataset` to create dataset from **your local directory**

```Python
dataset_data = {
    "data_source": DataSource.SDK,
    "storage_url" : "",
    "container_name": "",
    "sas_token":"",
    "data_folder": "/path/to/your_localdir",
    "name": "Dataset Local Upload",
    "type": DatasetType.ANNOTATED_DATA,
    "generate_metadata": False,
    "auto_tagging": ["weather"],
    "render_pcd": False,
    "annotation_format": AnnotationFormat.VISION_AI,
    "sequential": False,
    "sensors": project.sensors,
    "annotations" :['model_name']
}
dataset = project.create_dataset(**dataset_data)
```

## Get Dataset

The `get_dataset` method retrieves the dataset info from the connected site. The `id` parameter is the unique interger ID of the dataset, not its "name" property.

```Python
dataset = client.get_dataset(id)
```

### List Models
The `list_models` method will list all the models in the given project

```Python
#1
models = client.list_models(project_id = 1)
#2
project = client.get_project(project_id=1)
models = project.list_models()
```
### Get Model
The `get_model` method will get the model detail info by the given model-id

```Python
model = client.get_model(model_id=30)
model = project.get_model(model_id=30)
```
From the given model, we could get the label file and the triton model file by the commands below.
```Python
model.get_label_file()
model.get_triton_model_file()

```


## Troubleshooting


## Next steps


## Contributing



## Links to language repos

[Python Readme](https://github.com/linkernetworks/dataverse-sdk/tree/develop/python/README.md)

from setuptools import find_packages, setup

AUTHOR = "LinkerVision"
PACKAGE_NAME = "dataverse-sdk"
PACKAGE_VERSION = "0.1.0"
DESC = "Dataverse SDK For Python"
with open("README.md", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    packages=find_packages(),
    python_requires=">=3.9, <4",
    author=AUTHOR,
    url="",
    description=DESC,
    install_requires=["pydantic", "requests"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=["Programming Language :: Python :: 3"],
)

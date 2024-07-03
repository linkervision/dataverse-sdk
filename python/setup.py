from setuptools import find_packages, setup

AUTHOR = "LinkerVision"
PACKAGE_NAME = "dataverse-sdk"
PACKAGE_VERSION = "1.4.0"
DESC = "Dataverse SDK For Python"
with open("README.md", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    packages=find_packages(),
    python_requires=">=3.10, <4",
    author=AUTHOR,
    url="",
    description=DESC,
    install_requires=["pydantic==1.*", "requests", "httpx>=0.23.0"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=["Programming Language :: Python :: 3"],
)

import os.path as osp

from setuptools import find_packages, setup

PACKAGE_NAME = "dataverse_sdk"
PACKAGE_VERSION = "0.1.0"
ROOT = osp.abspath(osp.dirname(__file__))

with open(osp.join(ROOT, "requirements/requirements.txt")) as f:
    requires = f.read().split("\n")

setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    packages=find_packages(),
    python_requires=">=3.9, <4",
    author="LinkerVision",
    install_requires=[requires],
)

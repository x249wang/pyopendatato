# -*- coding: utf-8 -*-

import os

from setuptools import find_packages, setup

here = os.path.dirname(__file__)


def read_requirements(file_):
    with open(os.path.join(here, file_)) as f:
        return sorted(list(set(line.split("#")[0].strip() for line in f)))


install_requires = read_requirements("requirements.txt")


with open(os.path.join(here, "VERSION")) as f:
    version = f.read().strip()


setup(
    name="pyopendatato",
    version=version,
    description="Python tool for downloading data from City of Toronto's Open Data Portal",
    scripts=["pyopendatato"],
    author="Alex Wang",
    author_email="x249wang@uwaterloo.ca",
    install_requires=install_requires,
    packages=find_packages(exclude=["tests"]),
    url="https://github.com/x249wang/pyopendatato",
    include_package_data=True,
    license="mit",
    classifiers=[
        "Programming Language :: Python :: 3 :: Only",
        "License :: OSI Approved :: MIT License",
    ],
)

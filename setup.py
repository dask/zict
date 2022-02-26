#!/usr/bin/env python

import os

from setuptools import setup

setup(
    name="zict",
    version="2.1.0",
    description="Mutable mapping tools",
    url="http://zict.readthedocs.io/en/latest/",
    maintainer="Matthew Rocklin",
    maintainer_email="mrocklin@gmail.com",
    license="BSD",
    keywords="mutable mapping,dict,dask",
    packages=["zict"],
    python_requires=">=3.7",
    install_requires=open("requirements.txt").read().strip().split("\n"),
    long_description=(
        open("README.rst").read() if os.path.exists("README.rst") else ""
    ),
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    zip_safe=False,
)

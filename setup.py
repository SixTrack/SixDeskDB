#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ast
import os
from setuptools import setup, find_packages

from numpy.distutils.core import Extension, setup

ext1 = Extension(name = 'sixdeskdb.search',
                 sources = ['sixdeskdb/search.f90'])

VERSION = "0.0.0"

setup(
    name="sixdeskdb",
    version=VERSION,
    description="SixTrack Analysis",
    author="Riccardo De Maria",
    author_email="riccardo.de.maria@cern.ch",
    url="https://github.com/rdemaria/SixDeskDB",
    packages=find_packages(),
    ext_modules = [ext1],
    python_requires=">=3.6, <4",
    install_requires=[
        "matplotlib",
        "scipy",
        "numpy",
    ],
    extras_require={"dev": ["pytest"]},
)

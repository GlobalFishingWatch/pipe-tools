#!/usr/bin/env python

"""
Setup script for pipe-tools
"""
from pipe_tools.beam.requirements import requirements as DATAFLOW_PINNED_DEPENDENCIES
from setuptools import find_packages
from setuptools import setup

import codecs
import os
import sys

PACKAGE_NAME = 'pipe-tools'

package = __import__('pipe_tools')

DEPENDENCIES = [
    "newlinejson",
    # "nose",
    "pytest",
    # "python-dateutil",
    # "pytz",
    "udatetime",
    "ujson",
    "six>=1.12"
]

SCRIPTS = [
    'bin/pipe-tools-utils',
    'bin/xdaterange',
]

with codecs.open('README.md', encoding='utf-8') as f:
    readme = f.read().strip()

setup(
    author=package.__author__,
    author_email=package.__email__,
    description=package.__doc__.strip(),
    include_package_data=True,
    install_requires=DEPENDENCIES + DATAFLOW_PINNED_DEPENDENCIES,
    keywords='AIS GIS remote sensing',
    license=package.__license__.strip(),
    long_description=readme,
    name=PACKAGE_NAME,
    packages=find_packages(exclude=['test*.*', 'tests']),
    url=package.__source__,
    version=package.__version__,
    zip_safe=True,
    scripts=SCRIPTS
)

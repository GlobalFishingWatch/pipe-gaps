#!/usr/bin/env python

import setuptools
from pipe_gaps.version import __version__


setuptools.setup(
    name="pipe-gaps",
    version=__version__,
    author="Global Fishing Watch.",
    maintainer="Tomás J. Link",
    maintainer_email="tomas.link@globalfishingwatch.org",
    description="Time gap detector for AIS position messages.",
    long_description_content_type="text/markdown",
    url="https://github.com/GlobalFishingWatch/pipe-gaps",
    packages=setuptools.find_packages(),
    include_package_data=True,
    package_data={
        'pipe_gaps': [
            'data/*.json',
            'pipeline/schemas/*.json'
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "importlib-resources~=6.0",
        "geopy~=2.4",
        "google-cloud-bigquery~=3.0",
        "google-cloud-profiler~=4.1",
        "pandas~=2.1",
        "py-cpuinfo~=9.0",
        "pydantic~=2.0",
        "rich~=13.0",
        "sqlparse~=0.5",
    ],
    extras_require={
        "beam": ["apache-beam[gcp]~=2.63"],
    },
    entry_points={
        "console_scripts": [
            "pipe-gaps = pipe_gaps.cli:main",
        ]
    },
)

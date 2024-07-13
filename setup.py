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
        "google-cloud-bigquery~=3.0",
        "py-cpuinfo~=9.0",
        "rich~=13.0",
    ],
    extras_require={
        "beam": ["apache-beam[gcp, dataframe]~=2.0"],
    },
    entry_points={
        "console_scripts": [
            "pipe-gaps = pipe_gaps.cli:main",
        ]
    },
)

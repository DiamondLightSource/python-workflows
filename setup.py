from __future__ import absolute_import, division, print_function

import sys

from setuptools import find_packages, setup

if sys.version_info < (2, 7):
    sys.exit("Sorry, Python < 2.7 is not supported")

setup(
    name="workflows",
    description="Data processing in distributed environments",
    long_description="Workflows enables light-weight services to process tasks in a message-oriented environment.",
    url="https://github.com/DiamondLightSource/python-workflows",
    author="Markus Gerstel",
    author_email="scientificsoftware@diamond.ac.uk",
    download_url="https://github.com/DiamondLightSource/python-workflows/releases",
    version="1.5",
    install_requires=['enum34;python_version<"3.4"', "setuptools", "six", "stomp.py"],
    packages=find_packages(),
    license="BSD",
    entry_points={
        "workflows.services": [
            "SampleConsumer = workflows.services.sample_consumer:SampleConsumer",
            "SampleProducer = workflows.services.sample_producer:SampleProducer",
            "SampleTxn = workflows.services.sample_transaction:SampleTxn",
            "SampleTxnProducer = workflows.services.sample_transaction:SampleTxnProducer",
        ],
        "workflows.transport": [
            "StompTransport = workflows.transport.stomp_transport:StompTransport"
        ],
    },
    tests_require=["mock", "pytest"],
    zip_safe=False,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)

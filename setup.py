from setuptools import find_packages, setup

with open("README.rst") as readme_file:
    readme = readme_file.read()

setup(
    name="workflows",
    description="Data processing in distributed environments",
    long_description=readme,
    url="https://github.com/DiamondLightSource/python-workflows",
    author="Markus Gerstel",
    author_email="scientificsoftware@diamond.ac.uk",
    download_url="https://github.com/DiamondLightSource/python-workflows/releases",
    version="2.6",
    install_requires=["pika", "setuptools", "stomp.py<6.1.1"],
    python_requires=">=3.6",
    packages=find_packages(),
    license="BSD",
    entry_points={
        "console_scripts": [
            "workflows.validate_recipe = workflows.recipe.validate:main"
        ],
        "libtbx.dispatcher.script": [
            "workflows.validate_recipe = workflows.validate_recipe"
        ],
        "libtbx.precommit": ["workflows = workflows"],
        "workflows.services": [
            "SampleConsumer = workflows.services.sample_consumer:SampleConsumer",
            "SampleProducer = workflows.services.sample_producer:SampleProducer",
            "SampleTxn = workflows.services.sample_transaction:SampleTxn",
            "SampleTxnProducer = workflows.services.sample_transaction:SampleTxnProducer",
        ],
        "workflows.transport": [
            "StompTransport = workflows.transport.stomp_transport:StompTransport",
            "PikaTransport = workflows.transport.pika_transport:PikaTransport",
        ],
    },
    tests_require=["pytest"],
    zip_safe=False,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)

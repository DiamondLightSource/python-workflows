=========
Workflows
=========

.. image:: https://img.shields.io/pypi/v/workflows.svg
        :target: https://pypi.python.org/pypi/workflows
        :alt: PyPI release

.. image:: https://img.shields.io/conda/vn/conda-forge/workflows.svg
        :target: https://anaconda.org/conda-forge/workflows
        :alt: Conda version

.. image:: https://dev.azure.com/zocalo/python-zocalo/_apis/build/status/DiamondLightSource.python-workflows?branchName=main
        :target: https://dev.azure.com/zocalo/python-zocalo/_build/latest?definitionId=3&branchName=main
        :alt: Build status

.. image:: https://img.shields.io/pypi/l/workflows.svg
        :target: https://pypi.python.org/pypi/workflows
        :alt: BSD license

.. image:: https://img.shields.io/pypi/pyversions/workflows.svg
        :target: https://pypi.org/project/workflows/
        :alt: Supported Python versions

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
        :target: https://github.com/psf/black
        :alt: Code style: black

.. image:: https://img.shields.io/lgtm/grade/python/g/DiamondLightSource/python-workflows.svg?logo=lgtm&logoWidth=18
        :target: https://lgtm.com/projects/g/DiamondLightSource/python-workflows/context:python
        :alt: Language grade: Python

.. image:: https://img.shields.io/lgtm/alerts/g/DiamondLightSource/python-workflows.svg?logo=lgtm&logoWidth=18
        :target: https://lgtm.com/projects/g/DiamondLightSource/python-workflows/alerts/
        :alt: Total alerts

Workflows enables light-weight services to process tasks in a message-oriented
environment.

It is comprised of a communications layer (``workflows.transport``) that provides a
common interface to queues and topics over different transport providers, a
service abstraction layer (``workflows.frontend``) which encapsulates a service in
a separate process, tools for status reporting, logging and controlling
services via a transport mechanism, and a service class skeleton alongside
example services (``workflows.services.common_service`` et al.).
To achieve more complex workflows services can be interconnected using
`recipes`_ whereby the output of one service can be directed
onwards to other services.

.. _recipes: https://github.com/DiamondLightSource/python-workflows/tree/main/src/workflows/recipe/README.MD

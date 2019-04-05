[![PYPI release](https://img.shields.io/pypi/v/workflows.svg)](https://pypi.python.org/pypi/workflows)
[![Conda Version](https://img.shields.io/conda/vn/conda-forge/workflows.svg)](https://anaconda.org/conda-forge/workflows)
[![Build Status](https://travis-ci.org/DiamondLightSource/python-workflows.svg?branch=master)](https://travis-ci.org/DiamondLightSource/python-workflows)
[![Coverage Status](https://coveralls.io/repos/github/DiamondLightSource/python-workflows/badge.svg?branch=master)](https://coveralls.io/github/DiamondLightSource/python-workflows?branch=master)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/DiamondLightSource/python-workflows.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/DiamondLightSource/python-workflows/context:python)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/DiamondLightSource/python-workflows.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/DiamondLightSource/python-workflows/alerts/)


Data processing in distributed environments
===========================================

Workflows enables light-weight services to process tasks in a message-oriented
environment.

It is comprised of a communications layer (workflows.transport) that provides a
common interface to queues and topics over different transport providers, a
service abstraction layer (workflows.frontend) which encapsulates a service in
a separate process, tools for status reporting, logging and controlling
services via a transport mechanism, and a service class skeleton alongside
example services (workflows.services.common_service et al.).
To achieve more complex workflows services can be interconnected using
[recipes](recipe/README.MD) whereby the output of one service can be directed
onwards to other services.

[![PYPI release](https://img.shields.io/pypi/v/workflows.svg)](https://pypi.python.org/pypi/workflows)
[![Build Status](https://travis-ci.org/DiamondLightSource/python-workflows.svg?branch=master)](https://travis-ci.org/DiamondLightSource/python-workflows)
[![Coverage Status](https://coveralls.io/repos/github/DiamondLightSource/python-workflows/badge.svg?branch=master)](https://coveralls.io/github/DiamondLightSource/python-workflows?branch=master)

[![SonarQube Quality Gate](https://sonarcloud.io/api/project_badges/measure?project=uk.ac.diamond.workflows&metric=alert_status)](https://sonarcloud.io/dashboard?id=uk.ac.diamond.workflows)
[![SonarQube Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=uk.ac.diamond.workflows&metric=vulnerabilities)](https://sonarcloud.io/project/issues?id=uk.ac.diamond.workflows&resolved=false&types=VULNERABILITY)
[![SonarQube Bugs](https://sonarcloud.io/api/project_badges/measure?project=uk.ac.diamond.workflows&metric=bugs)](https://sonarcloud.io/project/issues?id=uk.ac.diamond.workflows&resolved=false&types=BUG)
[![SonarQube TechDebt](https://sonarcloud.io/api/project_badges/measure?project=uk.ac.diamond.workflows&metric=sqale_index)](https://sonarcloud.io/project/issues?facetMode=effort&id=uk.ac.diamond.workflows&resolved=false&types=CODE_SMELL)

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

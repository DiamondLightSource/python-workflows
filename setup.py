from __future__ import absolute_import, division, print_function

import io
import os
import re
import sys

from setuptools import find_packages, setup

# cf.
# https://packaging.python.org/guides/single-sourcing-package-version/#single-sourcing-the-version
def read(*names, **kwargs):
  with io.open(
    os.path.join(os.path.dirname(__file__), *names),
    encoding=kwargs.get("encoding", "utf8")
  ) as fp:
    return fp.read()

def find_version(*file_paths):
  version_file = read(*file_paths)
  version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                            version_file, re.M)
  if version_match:
    return version_match.group(1)
  raise RuntimeError("Unable to find version string.")

if sys.version_info < (2,7):
  sys.exit('Sorry, Python < 2.7 is not supported')

setup(name='workflows',
      description='Supervised data processing in distributed environments',
      url='https://github.com/DiamondLightSource/python-workflows',
      author='Markus Gerstel',
      author_email='scientificsoftware@diamond.ac.uk',
      download_url="https://github.com/DiamondLightSource/python-workflows/releases",
      version=find_version('workflows', '__init__.py'),
      install_requires=[
          'enum-compat',
          'six',
          'stomp.py',
      ],
      packages=find_packages(),
      license='BSD',
      entry_points={
        'workflows.services': [
          'SampleConsumer = workflows.services.sample_consumer:SampleConsumer',
          'SampleProducer = workflows.services.sample_producer:SampleProducer',
          'SampleTxn = workflows.services.sample_transaction:SampleTxn',
          'SampleTxnProducer = workflows.services.sample_transaction:SampleTxnProducer',
        ],
        'workflows.transport': [
          'StompTransport = workflows.transport.stomp_transport:StompTransport',
        ],
      },
      tests_require=['mock',
                     'pytest'],
      zip_safe=False,
      classifiers = [
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules'
        ]
     )

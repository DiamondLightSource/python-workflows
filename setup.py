from __future__ import absolute_import, division
from setuptools import setup, find_packages
import sys

package_version = '0.34'

# to release:
#  - increment number
#  - export NUMBER="0.34"
#  - git add -u; git commit -m "v${NUMBER} release"; git tag -a v${NUMBER} -m v${NUMBER}; git push; git push origin v${NUMBER}
#  - python setup.py register sdist upload

if sys.version_info < (2,7):
  sys.exit('Sorry, Python < 2.7 is not supported')

setup(name='workflows',
      description='Supervised data processing in distributed environments',
      url='https://github.com/DiamondLightSource/python-workflows',
      author='Markus Gerstel',
      author_email='anthchirp@users.noreply.github.com',
      download_url="https://github.com/DiamondLightSource/python-workflows/releases",
      version=package_version,
      install_requires=['stomp.py'],
      packages=find_packages(),
      license='BSD',
      tests_require=['mock'],
      zip_safe=False)

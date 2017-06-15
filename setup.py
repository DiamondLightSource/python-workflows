from __future__ import absolute_import, division
from setuptools import setup, find_packages
import sys

package_version = '0.47'

# to release:
#  - increment number
#  - export NUMBER="$(grep package_version setup.py | head -1 | cut -d"'" -f 2)"
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

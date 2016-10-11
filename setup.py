from setuptools import setup, find_packages

package_version = '0.6'

# to release:
#  - increment number
#  - git add -u; git commit -m "${number} release"
#  - git tag -a v${number}
#  - git push; git push origin v${number}
#  - python setup.py register sdist upload

setup(name='workflows',
      description='Supervised data processing in distributed environments',
      url='https://github.com/Anthchirp/workflows',
      author='Markus Gerstel',
      author_email='anthchirp@users.noreply.github.com',
      download_url="https://github.com/Anthchirp/workflows/releases",
      bugtrack_url="https://github.com/Anthchirp/workflows/issues",
      version=package_version,
      install_requires=['stomp.py'],
      packages=find_packages(),
      license='BSD',
      tests_require=['mock'],
      zip_safe=False)

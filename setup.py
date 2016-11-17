from setuptools import setup, find_packages

package_version = '0.15'

# to release:
#  - increment number
#  - export NUMBER="0.15"
#  - git add -u; git commit -m "v${NUMBER} release"; git tag -a v${NUMBER} -m v${NUMBER}; git push; git push origin v${NUMBER}
#  - python setup.py register sdist upload

setup(name='workflows',
      description='Supervised data processing in distributed environments',
      url='https://github.com/xia2/workflows',
      author='Markus Gerstel',
      author_email='anthchirp@users.noreply.github.com',
      download_url="https://github.com/xia2/workflows/releases",
      bugtrack_url="https://github.com/xia2/workflows/issues",
      version=package_version,
      install_requires=['stomp.py'],
      packages=find_packages(),
      license='BSD',
      tests_require=['mock'],
      zip_safe=False)

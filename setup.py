from setuptools import setup

package_version = '0.1'

setup(name='workflows',
      description='Supervised data processing in distributed environments',
      url='https://github.com/Anthchirp/workflows',
      author='Markus Gerstel',
      version=package_version,
      install_requires=['stomp.py'],
      packages=['workflows'],
      license='BSD',
      tests_require=['mock'],
      zip_safe=False)

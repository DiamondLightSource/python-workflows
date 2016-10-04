from setuptools import setup

setup(name='workflows',
      version='0.0',
      description='Supervised data processing in distributed environments',
      url='https://github.com/Anthchirp/workflows',
      author='Markus Gerstel',
      license='BSD',
      packages=['workflows'],
      install_requires=['stomp.py'],
      tests_require=['mock'],
      zip_safe=False)

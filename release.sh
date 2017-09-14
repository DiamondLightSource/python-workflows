#!/bin/bash
set -e

python -c "import sys; assert sys.hexversion >= 0x02070000, 'python too old'"
python -c "import wheel" || python -m pip install wheel
export NUMBER=$(python -c "import workflows; print workflows.__version__")
git tag -a v${NUMBER} -m v${NUMBER}
git push origin v${NUMBER}
python setup.py sdist bdist_wheel upload

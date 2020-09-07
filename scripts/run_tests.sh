#!/bin/bash
source ~/.bashrc

set -eu

cd ${HUNTSMAN_DRP}/src/huntsman/drp

pytest -x --cov-report html:htmlcov  --cov-config=.coveragerc

exit 0

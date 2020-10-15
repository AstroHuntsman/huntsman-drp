#!/bin/bash
source ~/.bashrc

set -eu

COVERAGE_REPORT_HTML=${HUNTSMAN_DRP}/htmlcov
COVERAGE_REPORT_XML=${HUNTSMAN_DRP}/coverage.xml
SESSION_FILE=~/pytest_session.txt

cd ${HUNTSMAN_DRP}/src/huntsman/drp

pytest -x

#pytest -x --cov=huntsman.drp --cov-report html:${COVERAGE_REPORT_HTML} \
#  --cov-config=.coveragerc --session2file=${SESSION_FILE}

exit 0

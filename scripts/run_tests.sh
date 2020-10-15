#!/bin/bash
source ~/.bashrc
set -eu

COVERAGE_REPORT_HTML=${HUNTSMAN_DRP}/htmlcov
COVERAGE_CONFIG=${HUNTSMAN_DRP}/src/huntsman/drp/.coveragerc
COVERAGE_REPORT_XML=${HUNTSMAN_DRP}/coverage.xml
SESSION_FILE=~/pytest_session.txt

# cd ${HUNTSMAN_DRP}/src/huntsman/drp
cd ${LSST_HOME}

pytest ${HUNTSMAN_DRP} -x --cov=huntsman.drp \
          --cov-config=${COVERAGE_CONFIG} \
          --cov-report xml:${COVERAGE_REPORT_XML} \
          --cov-report html:${COVERAGE_REPORT_HTML} \
          --session2file=${SESSION_FILE}

exit 0

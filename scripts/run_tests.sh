#!/bin/bash
source ~/.bashrc
set -eu

# Use a root where we are guaranteed to have write permissions
COVERAGE_ROOT=${LSST_HOME}

pytest ${HUNTSMAN_DRP} -x \
  --cov=huntsman.drp \
  --cov-config=${HUNTSMAN_DRP}/src/huntsman/drp/.coveragerc \
  --cov-report xml:${COVERAGE_ROOT}/coverage.xml \
  --session2file=${COVERAGE_ROOT}/pytest_session.txt

exit 0

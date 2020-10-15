#!/bin/bash
source ~/.bashrc
set -eu

# Coverage root set by first command line arg. If not given, use package root.
COVERAGE_ROOT=${1:-${HUNTSMAN_DRP}}
cd ${COVERAGE_ROOT}

pytest ${HUNTSMAN_DRP} -x \
  --cov=huntsman.drp \
  --cov-config=${HUNTSMAN_DRP}/src/huntsman/drp/.coveragerc \
  --cov-report xml:${COVERAGE_ROOT}/coverage.xml \
  --cov-report html:${COVERAGE_ROOT}/htmlcov \
  --session2file=${COVERAGE_ROOT}/pytest_session.txt

exit 0

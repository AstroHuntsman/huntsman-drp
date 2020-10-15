#!/bin/bash
set -eu

LSST_HOME=/opt/lsst/software/stack

docker-compose run --rm \
  -e "HUNTSMAN_COVERAGE=${LSST_HOME}/coverage" \
  -v ${COVERAGE_DIR}:${LSST_HOME}/coverage \
  python_tests ${LSST_HOME}/huntsman-drp/scripts/testing/run_local_tests.sh

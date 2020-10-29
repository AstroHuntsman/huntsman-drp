#!/bin/bash
# This script is run from outside of the docker environment and is useful for running
# local tests.
set -eu

COVERAGE_DIR=${HUNTSMAN_DRP_COVDIR:-${HUNTSMAN_DRP}/coverage}
COMPOSE_FILE=${HUNTSMAN_DRP}/docker/testing/docker-compose.yml

mkdir -p ${COVERAGE_DIR}

docker-compose -f ${COMPOSE_FILE} run --rm \
  -e "HUNTSMAN_COVERAGE=/opt/lsst/software/stack/coverage" \
  -v "${COVERAGE_DIR}:/opt/lsst/software/stack/coverage" \
  python_tests

docker-compose -f ${COMPOSE_FILE} down

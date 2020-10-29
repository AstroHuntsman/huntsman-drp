#!/bin/bash
# This script is run from outside of the docker environment and is useful for running
# local tests.
set -eu

HUNTSMAN_DRP_COVDIR=${HUNTSMAN_DRP_COVDIR:-${HUNTSMAN_DRP}/coverage}
COMPOSE_FILE=${HUNTSMAN_DRP}/docker/testing/docker-compose.yml

function cleanup {
  echo "Stopping docker testing services."
  docker-compose -f ${COMPOSE_FILE} down
}

mkdir -p ${COVERAGE_DIR}

trap cleanup EXIT
docker-compose -f ${COMPOSE_FILE} run --rm \
  -e "HUNTSMAN_COVERAGE=/opt/lsst/software/stack/coverage" \
  -v "${HUNTSMAN_DRP_COVDIR}:/opt/lsst/software/stack/coverage" \
  python_tests

#!/bin/bash
# This script is run from outside of the docker environment
set -eu

COMPOSE_FILE=${HUNTSMAN_DRP}/docker/testing/docker-compose.yml
HUNTSMAN_DRP_LOGS=${HUNTSMAN_DRP_LOGS:-${HUNTSMAN_DRP}/logs}

function cleanup {
  echo "Stopping docker testing services."
  docker-compose -f ${COMPOSE_FILE} down
}

mkdir -p ${HUNTSMAN_DRP_COVDIR} && chmod -R 777 ${HUNTSMAN_DRP_COVDIR}

echo "Building new docker image(s) for testing..."
docker-compose -f ${COMPOSE_FILE} build python-tests

echo "Running python tests inside docker container..."
echo "Local log directory: ${HUNTSMAN_DRP_LOGS}"

trap cleanup EXIT
docker-compose -f ${COMPOSE_FILE} run --rm python-tests

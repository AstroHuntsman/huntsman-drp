#!/usr/bin/env bash
set -e

source ${HUNTSMAN_DRP}/docker/bash-config.sh
python -c "from huntsman.drp.services.health import HealthMonitor; h = HealthMonitor(); h.start()"

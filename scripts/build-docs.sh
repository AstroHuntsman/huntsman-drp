#!/usr/bin/env bash
set -e

# Source the LSST env
# This is required because Sphinx needs to import modules
source ${HUNTSMAN_DRP}/docker/bash-config.sh

# Install the docs theme
pip install sphinx_rtd_theme

set -eu

# Build the docs
cd ${HUNTSMAN_DRP}/docs
sphinx-build -b html ./source ./build/html

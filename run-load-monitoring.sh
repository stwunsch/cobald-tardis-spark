#!/bin/bash

SCRIPT=$(readlink -f $BASH_SOURCE[0])
SCRIPT_DIR=$(dirname $SCRIPT)
source $SCRIPT_DIR/setup.sh

NAME=$(hostname)

unbuffer $PYTHON_BINARY run-load-monitoring.py | tee $PROJECT_TMP/load-monitoring-$NAME.log

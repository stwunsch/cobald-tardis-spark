#!/bin/bash

SCRIPT=$(readlink -f $BASH_SOURCE[0])
SCRIPT_DIR=$(dirname $SCRIPT)
source $SCRIPT_DIR/setup.sh

unbuffer $PYTHON_BINARY run-monitoring.py | tee $PROJECT_TMP/monitoring.log

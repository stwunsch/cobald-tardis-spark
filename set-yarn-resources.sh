#!/bin/bash

SCRIPT=$(readlink -f $BASH_SOURCE[0])
SCRIPT_DIR=$(dirname $SCRIPT)
source $SCRIPT_DIR/setup.sh

$PYTHON_BINARY $SCRIPT_DIR/set-yarn-resources.py $CORES $MEMORY

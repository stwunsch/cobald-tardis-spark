#!/bin/bash

SCRIPT=$(readlink -f $BASH_SOURCE[0])
SCRIPT_DIR=$(dirname $SCRIPT)
source $SCRIPT_DIR/setup.sh

yarn resourcemanager 2>&1 | tee $PROJECT_TMP/resourcemanager-$(hostname).log

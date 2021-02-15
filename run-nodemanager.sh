#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_DIR=$(dirname $SCRIPT)
source $SCRIPT_DIR/setup.sh

yarn nodemanager 2>&1 | tee $PROJECT_TMP/nodemanager-$(hostname).log

#!/bin/bash

SCRIPT=$(readlink -f $BASH_SOURCE[0])
SCRIPT_DIR=$(dirname $SCRIPT)
source $SCRIPT_DIR/setup.sh

# Recreate nodemanager database
rm -f $COBALD_TARDIS_NODEMANAGER_DATABASE
$PYTHON_BINARY $COBALD_TARDIS_CONFIG_DIR/nodemanager_database.py $COBALD_TARDIS_NODEMANAGER_DATABASE --create

yarn resourcemanager 2>&1 | tee $PROJECT_TMP/resourcemanager-$(hostname).log

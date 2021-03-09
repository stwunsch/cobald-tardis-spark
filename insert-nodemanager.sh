#!/bin/bash

SCRIPT=$(readlink -f $BASH_SOURCE[0])
SCRIPT_DIR=$(dirname $SCRIPT)
source $SCRIPT_DIR/setup.sh

$PYTHON_BINARY $COBALD_TARDIS_CONFIG_DIR/nodemanager_database.py $COBALD_TARDIS_NODEMANAGER_DATABASE --insert_nm

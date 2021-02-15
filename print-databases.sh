#!/bin/bash

SCRIPT=$(readlink -f $BASH_SOURCE[0])
SCRIPT_DIR=$(dirname $SCRIPT)
source $SCRIPT_DIR/setup.sh

echo ">>> Nodemanagers"
$PYTHON_BINARY $COBALD_TARDIS_CONFIG_DIR/nodemanager_database.py $COBALD_TARDIS_NODEMANAGER_DATABASE --print
echo
echo ">>> Drones"
$PYTHON_BINARY $COBALD_TARDIS_CONFIG_DIR/drones_database.py $COBALD_TARDIS_DRONES_DATABASE --print

#!/bin/bash

SCRIPT=$(readlink -f $BASH_SOURCE[0])
SCRIPT_DIR=$(dirname $SCRIPT)
source $SCRIPT_DIR/setup.sh

cd $COBALD_TARDIS_CONFIG_DIR

# Recreate drones database
rm -f $COBALD_TARDIS_DRONES_DATABASE
$PYTHON_BINARY drones_database.py $COBALD_TARDIS_DRONES_DATABASE --create

# Run cobald
$PYTHON_BINARY -m cobald.daemon cobald.yml 2>&1 | tee $PROJECT_TMP/cobald.log

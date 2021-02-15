#!/bin/bash

source setup.sh

cd $COBALD_TARDIS_CONFIG_DIR

# Recreate databases
rm -f $COBALD_TARDIS_DRONES_DATABASE $COBALD_TARDIS_NODEMANAGER_DATABASE
$PYTHON_BINARY nodemanager_database.py --create
$PYTHON_BINARY drones_database.py --create

# Run cobald
$PYTHON_BINARY -m cobald.daemon cobald.yml 2>&1 | tee $PROJECT_TMP/cobald.log

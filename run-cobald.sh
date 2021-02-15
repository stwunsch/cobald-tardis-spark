#!/bin/bash

source setup.sh

cd $COBALD_TARDIS_CONFIG_DIR
$PYTHON_BINARY -m cobald.daemon cobald.yml 2>&1 | tee $PROJECT_TMP/cobald.log

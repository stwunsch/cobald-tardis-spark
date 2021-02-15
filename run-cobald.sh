#!/bin/bash

source setup.sh

$PYTHON_BINARY -m cobald.daemon $COBALD_TARDIS_CONFIG_DIR/cobald.yml 2>&1 | tee $PROJECT_TMP/cobald.log

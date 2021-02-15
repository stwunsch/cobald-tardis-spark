#!/bin/bash

# General settings
SCRIPT=$(readlink -f $0)
SCRIPT_DIR=$(dirname $SCRIPT)
source $SCRIPT_DIR/config.sh

# Python
source $PROJECT_HOME/pyvenv/bin/activate

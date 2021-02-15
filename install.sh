#!/bin/bash

# Import settings like project home, path to python binary, ...
source config.sh

# Setup python virtual environment
$PYTHON_BINARY -m venv $PROJECT_HOME/pyvenv

# Install all the required packages
source $PROJECT_HOME/pyvenv/bin/activate
pip install --upgrade pip
pip install -f $PROJECT_HOME/requirements.txt
pip install -e $PROJECT_HOME/cobald
pip install -e $PROJECT_HOME/tardis

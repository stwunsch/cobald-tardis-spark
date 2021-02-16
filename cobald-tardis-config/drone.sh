#!/bin/bash

echo "### Begin of job"

set -e

echo "Hostname:" `hostname`

echo "Who am I?" `id`

echo "Where am I?" `pwd`

echo "What is my system?" `uname -a`

echo "### Setup software"

source CONFIGURE_PROJECT_HOME/setup.sh

echo "### Environment"

echo "PATH=$PATH"
echo "PYTHONPATH=$PYTHONPATH"
echo "Python binary:" $(which python)
echo "Python version:" $(python --version)
echo "Tardis drone UUID:" ${TardisDroneUuid}
echo "Tardis Cores:" ${Cores}
echo "Tardis Memory:" ${Memory}
# TODO: Make this more generic, we specialize here on the sg machines!
NODEMANAGER_HOSTNAME==$(echo $(hostname) | sed -En "s/.*(sg.*)/\1/p")
echo "Yarn nodemanager:" ${NODEMANAGER_HOSTNAME}

echo "### Start working"

$PYTHON_BINARY $COBALD_TARDIS_CONFIG_DIR/drone.py ${TardisDroneUuid} ${NODEMANAGER_HOSTNAME} ${Cores} ${Memory}

echo "### End of job"

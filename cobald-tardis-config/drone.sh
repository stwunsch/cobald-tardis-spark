#!/bin/bash

echo "### Begin of job"

set -e

echo "Hostname:" `hostname`

echo "Who am I?" `id`

echo "Where am I?" `pwd`

echo "What is my system?" `uname -a`

echo "### Setup software"

source /work/wunsch/cobald-tardis-spark/setup.sh

echo "### Environment"

echo "PATH=$PATH"
echo "PYTHONPATH=$PYTHONPATH"
echo "Python binary:" $(which python)
echo "Python version:" $(python --version)
echo "Tardis drone UUID:" ${TardisDroneUuid}
echo "Tardis Cores:" ${Cores}
echo "Tardis Memory:" ${Memory}

echo "### Start working"

$PYTHON_BINARY $COBALD_TARDIS_CONFIG_DIR/yarn-logicalnode.py ${TardisDroneUuid} ${Cores} ${Memory}

echo "### End of job"

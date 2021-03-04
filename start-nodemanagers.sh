#!/bin/bash

SCRIPT=$(readlink -f $BASH_SOURCE[0])
SCRIPT_DIR=$(dirname $SCRIPT)

MODE=$1

HOSTNAMES=(
    sg01.etp.kit.edu
    sg02.etp.kit.edu
    sg03.etp.kit.edu
    sg04.etp.kit.edu
    ms02.etp.kit.edu
    ms03.etp.kit.edu
    ms04.etp.kit.edu
)

USER=$(whoami)

for HOSTNAME in ${HOSTNAMES[@]}
do
    if [ ${MODE} = "start" ]
    then
        echo "Start nodemanager on" ${HOSTNAME}
        ssh ${USER}@${HOSTNAME} "screen -d -m ${SCRIPT_DIR}/run-nodemanager.sh"
    elif  [ ${MODE} = "status" ]
    then
        echo "Show screen sessions on" ${HOSTNAME}
        ssh ${USER}@${HOSTNAME} "screen -ls"
    elif  [ ${MODE} = "stop" ]
    then
        echo "Kill screen sessions on" ${HOSTNAME}
        ssh ${USER}@${HOSTNAME} "killall screen"
    else
        echo "Unknown mode: " ${MODE}
        exit
    fi
done

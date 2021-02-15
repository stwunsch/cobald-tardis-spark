#!/bin/bash

# Project home (set here to this directory)
SCRIPT=$(readlink -f $BASH_SOURCE[0])
SCRIPT_DIR=$(dirname $SCRIPT)
export PROJECT_HOME=$SCRIPT_DIR
export PROJECT_TMP=$PROJECT_HOME/tmp

# Python
export PYTHON_BINARY=python3

# Cobald/Tardis
export COBALD_TARDIS_CONFIG_DIR=$PROJECT_HOME/cobald-tardis-config
export COBALD_TARDIS_DRONES_DATABASE=$PROJECT_TMP/drones.db
export COBALD_TARDIS_NODEMANAGER_DATABASE=$PROJECT_TMP/nodemanagers.db
export COBALD_TARDIS_CONDOR_LOG_DIR=$PROJECT_TMP/condor-logs

# Spark
export JAVA_HOME=/usr
export SPARK_HOME=$PROJECT_HOME/spark
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
export PYSPARK_PYTHON=$PYTHON_BINARY
export PYSPARK_DRIVER_PYTHON=$PYTHON_BINARY

# Hadoop
export HADOOP_HOME=$PROJECT_HOME/hadoop
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
export HADOOP_CONF_DIR=$PROJECT_HOME/hadoop-config

# Hostnames
export YARN_RESOURCEMANAGER='portal1'

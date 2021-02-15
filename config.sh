#!/bin/bash

# Project home (most likely this directory)
export PROJECT_HOME=$(pwd)
export PROJECT_LOGS=$PROJECT_HOME/logs

# Python
export PYTHON_BINARY=python3

# Cobald/Tardis
export COBALD_TARDIS_CONFIG_DIR=$PROJECT_HOME/cobald-tardis-config

# Spark
export JAVA_HOME=/usr
export SPARK_HOME=$PROJECT_HOME/spark
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH

# Hadoop
export HADOOP_HOME=$PROJECT_HOME/hadoop
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
export HADOOP_CONF_DIR=$PROJECT_HOME/hadoop-config

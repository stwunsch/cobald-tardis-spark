#!/bin/bash

source config.sh

# Install required software
$PROJECT_HOME/install_python.sh
$PROJECT_HOME/install_spark_hadoop.sh

# Set variables in config.sh to config scripts in Spark and Hadoop
$PROJECT_HOME/configure.sh

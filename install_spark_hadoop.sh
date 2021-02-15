#!/bin/bash

source config.sh

# Spark
NAME=spark-3.0.1-bin-hadoop3.2
curl -Os https://downloads.apache.org/spark/spark-3.0.1/$NAME.tgz
tar xvf $NAME.tgz
rm $NAME.tgz
mv $NAME spark/

# Hadoop suite
NAME=hadoop-3.3.0
curl -Os https://downloads.apache.org/hadoop/common/$NAME/$NAME.tar.gz
tar xvf $NAME.tar.gz
rm $NAME.tar.gz
mv $NAME hadoop/

# Copy the library for the YarnShuffleService at the right place
cp spark/yarn/spark-3.0.1-yarn-shuffle.jar hadoop/share/hadoop/yarn/lib

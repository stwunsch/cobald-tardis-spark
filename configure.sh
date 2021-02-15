#!/bin/bash

source config.sh

# Hadoop
sed -i 's,CONFIGURE_YARN_LOG_DIR,'$PROJECT_TMP'/yarn-logs,g' $HADOOP_CONF_DIR/yarn-env.sh
sed -i 's,CONFIGURE_YARN_LOCAL_DIR,'$PROJECT_TMP'/yarn-local-dir,g' $HADOOP_CONF_DIR/yarn-site.xml
sed -i 's,CONFIGURE_YARN_RESOURCEMANAGER,'$YARN_RESOURCEMANAGER',g' $HADOOP_CONF_DIR/yarn-site.xml

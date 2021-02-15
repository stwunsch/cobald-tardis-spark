#!/bin/bash

source setup.sh

yarn resourcemanager 2>&1 | tee $PROJECT_TMP/resourcemanager-$(hostname).log

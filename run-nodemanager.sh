#!/bin/bash

source setup.sh

yarn nodemanager 2>&1 | tee $PROJECT_TMP/nodemanager-$(hostname).log

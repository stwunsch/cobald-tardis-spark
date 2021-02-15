#!/bin/bash

rm -f yarn_nm.db yarn_drones.db

python yarn_nm_db.py --create
python yarn_drones_db.py --create

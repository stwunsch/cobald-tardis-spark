#!/bin/bash

echo ">>> Nodemanagers"
python yarn_nm_db.py --print

echo ">>> Drones"
python yarn_drones_db.py --print

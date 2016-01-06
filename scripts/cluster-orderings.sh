#!/bin/bash

NO_CLUSTERS=50
INDEX_PREFIX=/mnt/eldysk/web/gov2c/gov2
GLOBAL_ORDERING=/mnt/eldysk/web/gov2c/ordering
OUTPUT_PREFIX=/mnt/eldysk/web/gov2c/map

for i in $(seq 1 ${NO_CLUSTERS}); do
    ./cluster-ordering.sh "${INDEX_PREFIX}-${i}.titles" ${GLOBAL_ORDERING} "${OUTPUT_PREFIX}-${i}"
done
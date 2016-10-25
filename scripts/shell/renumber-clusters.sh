#!/bin/bash

NO_CLUSTERS=50
INPUT_PREFIX=/mnt/eldysk/web/gov2c/gov2
OUTPUT_PREFIX=/mnt/eldysk/web/gov2c/gov2-R
FIELD=text
MAPPING_PREFIX=/mnt/eldysk/web/gov2c/map

for i in $(seq 1 ${NO_CLUSTERS}); do
    echo "Renumbering cluster ${i}"
    renumber.sh "${INPUT_PREFIX}-${i}" "${OUTPUT_PREFIX}-${i}" ${FIELD} "${MAPPING_PREFIX}-${i}"
done
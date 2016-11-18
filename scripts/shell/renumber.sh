#!/bin/bash

#
# Copy ${INPUT_INDEX} to ${OUTPUT_INDEX} (with field ${FIELD})
# and renumber its documents according to ${MAP}.
#

# Input index basename.
INPUT_INDEX=$1
# Output index basename.
OUTPUT_INDEX=$2
# Field name
FIELD=$3
# Mapping: an integer M at line N means renumbering document ID from N to M in the new index.
MAP=$4

if [ -z "${INPUT_INDEX}" ]; then echo "Variable INPUT_INDEX is not defined."; exit 1; fi;
if [ -z "${OUTPUT_INDEX}" ]; then echo "Variable OUTPUT_INDEX is not defined."; exit 1; fi;
if [ -z "${FIELD}" ]; then echo "Variable FIELD is not defined."; exit 1; fi;
if [ -z "${MAP}" ]; then echo "Variable MAP is not defined."; exit 1; fi;

# Titles need to be mapped manually.
INPUT_TITLES=${INPUT_INDEX}.titles
OUTPUT_TITLES=${OUTPUT_INDEX}.titles

starttime=$(date +%s)

export CLASSPATH=.:$(find ../ -iname \*.jar | paste -d: -s)

# The following code permutes the original titles according to the mapping and outputs to a new file.

# A file containing numbers from 0 to {numberOfDocuments}.
ID=tmp.id
seq 0 $((`wc -l < ${MAP}` - 1)) > ${ID}

paste ${ID} ${MAP} > tmp.a
paste ${ID} ${INPUT_TITLES} > tmp.b
join tmp.a tmp.b | cut -d " " -f 2- | sort -n | cut -d " " -f 2 > ${OUTPUT_TITLES}
rm tmp.*

java -Xmx1G edu.nyu.tandon.tool.renumber.Renumber -i "${INPUT_INDEX}-${FIELD}" -o "${OUTPUT_INDEX}-${FIELD}" -m "${MAP}"

endtime=$(date +%s)

echo "Renumbering time: $((endtime-starttime))s"

#!/bin/bash

#
# Copy ${INPUT_INDEX} to ${OUTPUT_INDEX} (with field ${FIELD})
# and renumber its documents according to ${TITLES}.
#

# Input index basename.
INPUT_INDEX=$1
# Output index basename.
OUTPUT_INDEX=$2
# Field name
FIELD=$3
# Reordered titles
TITLES=$4

if [ -z "${INPUT_INDEX}" ]; then echo "Variable INPUT_INDEX is not defined."; exit 1; fi;
if [ -z "${OUTPUT_INDEX}" ]; then echo "Variable OUTPUT_INDEX is not defined."; exit 1; fi;
if [ -z "${FIELD}" ]; then echo "Variable FIELD is not defined."; exit 1; fi;
if [ -z "${TITLES}" ]; then echo "Variable TITLES is not defined."; exit 1; fi;

INPUT_TITLES=${INPUT_INDEX}.titles
OUTPUT_TITLES=${OUTPUT_INDEX}.titles

starttime=$(date +%s)



ID=`mktemp`
seq 0 $((`wc -l < ${INPUT_TITLES}` - 1)) > ${ID}
# Produce pairs (title, originalID)
A=`mktemp`
paste -d" " ${INPUT_TITLES} ${ID} | sort -k1b,1 > ${A}
# Produce pairs (title, order)
B=`mktemp`
paste -d " " ${TITLES} ${ID} | sort -k1b,1 > ${B}

MAP=`mktemp`
join ${A} ${B} | cut -d " " -f 2- | sort -n -k2 | paste -d " " - ${ID} | sort -n -k1 | cut -d " " -f 3- > ${MAP}



cp ${TITLES} ${OUTPUT_TITLES}
java edu.nyu.tandon.tool.renumber.Renumber -i "${INPUT_INDEX}-${FIELD}" -o "${OUTPUT_INDEX}-${FIELD}" -m "${MAP}"

endtime=$(date +%s)

echo "Renumbering time: $((endtime-starttime))s"

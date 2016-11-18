#!/bin/bash

#
# Create an ordering ${OUTPUT} for a cluster,
# based on a global ordering ${ORDER} and cluster's ${TITLES}.
#

TITLES=$1
ORDER=$2
OUTPUT=$3

if [ -z "${TITLES}" ]; then echo "Variable TITLE is not defined."; exit 1; fi;
if [ -z "${ORDER}" ]; then echo "Variable ORDER is not defined."; exit 1; fi;
if [ -z "${OUTPUT}" ]; then echo "Variable OUTPUT is not defined."; exit 1; fi;

export LC_ALL=C

ID=id.tmp
seq 0 $((`wc -l < ${TITLES}` - 1)) > ${ID}

# Produce pairs (title, originalID)
paste -d " " ${TITLES} ${ID} | sort - > a.tmp

# Produce pairs (title, order)
if [ ! -f "b.tmp" ]
then
  seq 0 $((`wc -l < ${ORDER}` - 1)) | paste -d " " ${ORDER} - | sort - > b.tmp
fi

join a.tmp b.tmp | cut -d " " -f 2- | sort -n -k2 | paste -d " " - ${ID} | sort -n | cut -d " " -f 3- > ${OUTPUT}

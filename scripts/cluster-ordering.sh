#!/bin/bash

TITLES=$1
ORDER=$2
OUTPUT=$3

if [ -z "${TITLES}" ]; then echo "Variable TITLE is not defined."; exit 1; fi;
if [ -z "${ORDER}" ]; then echo "Variable ORDER is not defined."; exit 1; fi;
if [ -z "${OUTPUT}" ]; then echo "Variable OUTPUT is not defined."; exit 1; fi;
#TITLES=/mnt/eldysk/web/gov2c/gov2-33.titles
#ORDER=/mnt/eldysk/web/gov2c/ordering
#OUTPUT=/mnt/eldysk/web/gov2c/map33

ID=id.tmp
seq 0 $((`wc -l < ${TITLES}` - 1)) > ${ID}

# Produce pairs (title, originalID)
paste -d " " ${TITLES} ${ID} | sort - > a.tmp

# Produce pairs (title, order)
seq 0 $((`wc -l < ${ORDER}` - 1)) | paste -d " " ${ORDER} - | sort - > b.tmp

join a.tmp b.tmp | cut -d " " -f 2- | sort -n -k2 | paste -d " " - ${ID} | sort -n | cut -d " " -f 3- > ${OUTPUT}

rm *.tmp
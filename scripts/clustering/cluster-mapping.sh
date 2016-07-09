#!/bin/bash

#
# cluster-mapping.sh
#
# Produce a file containing IDs of the documents from a global index
# that are supposed to be contained in a cluster.
#
# Arguments:
# 1) the global index's *.titles file,
#    OR (if -c flag defined) a sorted mapping between titles and document IDs
#    (space separated values and new line delimited records);
# 2) the titles of the documents that are supposed to be in the cluster (new line delimited).
#

source "${MG4J_NYU_SCRIPTS}/commons.sh"

SORT=true
while getopts ":s" opt; do
  case $opt in
    s)
      SORT=false
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      ;;
  esac
done

shift $((OPTIND-1))

GLOBAL=$1
CLUSTER=$2
OUTPUT=$3
OUTPUT_TITLES=$4

if [ -z "${GLOBAL}" ]; then echo "You have to define global index's title list."; exit 1; fi;
if [ -z "${CLUSTER}" ]; then echo "You have to define cluster's title list."; exit 1; fi;
if [ -z "${OUTPUT}" ]; then echo "You have to define numbers output file."; exit 1; fi;
if [ -z "${OUTPUT_TITLES}" ]; then echo "You have to define titles output file."; exit 1; fi;

globalSorted=${GLOBAL}
if [ "$SORT" = true ]; then
        globalSorted=`mktemp`
        seq 0 $((`wc -l < ${GLOBAL}` - 1)) | paste -d" " ${GLOBAL} - | sort -k1b,1 > ${globalSorted}
fi
sort -k1b,1 ${CLUSTER} | join ${globalSorted} - | cut -d" " -f2 | sort -n > ${OUTPUT}
sort -k1b,1 ${CLUSTER} | join ${globalSorted} - | sort -n -k2 | cut -d" " -f1 > ${OUTPUT_TITLES}


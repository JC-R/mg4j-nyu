#!/bin/bash

#
# Run queries for all clusters in a directory.
#
# Arguments:
# 1) index
# 2) input with queries
# 3) output directory
#

source "${MG4J_NYU_SCRIPTS}/commons.sh"

index=$1
input=$2
outputDir=$3

if [ -z "${index}" ]; then echo "You have to define index (1)"; exit 1; fi;
if [ -z "${input}" ]; then echo "You have to define input file (2)"; exit 1; fi;
if [ -z "${outputDir}" ]; then echo "You have to define output directory (3)"; exit 1; fi;

inputBase=`basename ${input}`

java edu.nyu.tandon.experiments.cluster.ExtractClusterFeatures \
    -i ${input} \
    -o "${outputDir}/${inputBase}" \
    -k 500 \
    ${index}

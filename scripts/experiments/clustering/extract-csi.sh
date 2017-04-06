#!/bin/bash

#
# run-queries-for-clusters.sh
#
# Run queries for all clusters in a directory.
#
# Arguments:
# 1) cluster directory
# 2) input with queries
# 3) output directory
# 4) csi
#

source "${MG4J_NYU_SCRIPTS}/commons.sh"

dir=$1
input=$2
outputDir=$3
csiBase=$4

if [ -z "${dir}" ]; then echo "You have to define cluster directory."; exit 1; fi;
if [ -z "${input}" ]; then echo "You have to define input file."; exit 1; fi;
if [ -z "${outputDir}" ]; then echo "You have to define output directory."; exit 1; fi;
if [ -z "${csiBase}" ]; then echo "You have to define CSI."; exit 1; fi;

inputBase=`basename ${input}`
base="${dir}/`ls ${dir} | egrep '\.strategy' | sed 's/\.strategy//'`"
strategy="${dir}/`ls ${dir} | egrep '\.strategy'`"

starttime=$(date +%s)

set -e

java edu.nyu.tandon.experiments.cluster.ExtractShardScores \
    -s redde \
    -i ${input} \
    -o "${outputDir}/${inputBase}" \
    -c `ls ${dir}/*-*terms | wc -l` \
    -L 10 -L 20 -L 50 -L 100 -L 200 -L 500 -L 1000 \
    ${base} \
    ${csiBase}

java edu.nyu.tandon.experiments.cluster.ExtractShardScores \
    -s ranks \
    -i ${input} \
    -o "${outputDir}/${inputBase}" \
    -c `ls ${dir}/*-*terms | wc -l` \
    -L 10 -L 20 -L 50 -L 100 -L 200 -L 500 -L 1000 \
    ${base} \
    ${csiBase}

endtime=$(date +%s)

echo "Extracting time: $((endtime-starttime))s"
#!/bin/bash

fullIndex=$1
clusterDir=$2
csi=$3
input=$4
outputDir=$5
shift 5

if [ -z "${fullIndex}" ]; then echo "You have to define full index (1)"; exit 1; fi;
if [ -z "${clusterDir}" ]; then echo "You have to define cluster directory (2)"; exit 1; fi;
if [ -z "${csi}" ]; then echo "You have to define CSI (3)"; exit 1; fi;
if [ -z "${input}" ]; then echo "You have to define input file (4)"; exit 1; fi;
if [ -z "${outputDir}" ]; then echo "You have to define output directory (6)"; exit 1; fi;
if [ "$#" == "0" ]; then echo "You have to define a list of bucketization factors to test."; exit 1; fi

inputBase=`basename ${input}`

set -x
set -e

$MG4J_NYU_SCRIPTS/experiments/run-queries-for-index.sh ${fullIndex} ${input} ${outputDir}

java edu.nyu.tandon.experiments.cluster.ExtractClusterFeatures \
    -i ${input} \
    -o "${outputDir}/${inputBase}" \
    -f 0 \
    -b 1000 \
    -k 500 \
    ${fullIndex}
java edu.nyu.tandon.experiments.cluster.ExtractBucketizedPostingCost \
    -i ${input} \
    -o "${outputDir}/${inputBase}" \
    -b 1000 \
    ${fullIndex}

$MG4J_NYU_SCRIPTS/experiments/clustering/extract-taily.sh ${clusterDir} ${input} ${outputDir}
$MG4J_NYU_SCRIPTS/experiments/clustering/extract-csi.sh ${clusterDir} ${input} ${outputDir} ${csi}

while (( "$#" )); do

$MG4J_NYU_SCRIPTS/experiments/clustering/extract-posting-cost.sh ${clusterDir} ${input} ${outputDir} $1
$MG4J_NYU_SCRIPTS/experiments/clustering/run-queries-for-clusters.sh ${clusterDir} ${input} ${outputDir} $1

shift

done

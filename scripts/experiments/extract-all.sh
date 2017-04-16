#!/bin/bash

fullIndex=$1
clusterDir=$2
csi=$3
input=$4
tailyInput=$5
outputDir=$6
shift 6

if [ -z "${fullIndex}" ]; then echo "You have to define full index (1)"; exit 1; fi;
if [ -z "${clusterDir}" ]; then echo "You have to define cluster directory (2)"; exit 1; fi;
if [ -z "${csi}" ]; then echo "You have to define CSI (3)"; exit 1; fi;
if [ -z "${input}" ]; then echo "You have to define input file (4)"; exit 1; fi;
if [ -z "${tailyInput}" ]; then echo "You have to define Taily input file (5)"; exit 1; fi;
if [ -z "${outputDir}" ]; then echo "You have to define output directory (6)"; exit 1; fi;
if [ "$#" == "0" ]; then echo "You have to define a list of bucketization factors to test."; exit 1; fi

inputBase=`basename ${input}`
tailyInputBase=`basename ${tailyInput}`

set -x
set -e

$MG4J_NYU_SCRIPTS/experiments/run-queries-for-index.sh ${fullIndex} ${input} ${outputDir}
$MG4J_NYU_SCRIPTS/experiments/clustering/extract-taily.sh ${clusterDir} ${tailyInput} ${outputDir}
mv "${outputDir}/${tailyInputBase}.taily" "${outputDir}/${inputBase}.taily"
$MG4J_NYU_SCRIPTS/experiments/clustering/extract-csi.sh ${clusterDir} ${input} ${outputDir} ${csi}

while (( "$#" )); do

$MG4J_NYU_SCRIPTS/experiments/clustering/extract-posting-cost.sh ${clusterDir} ${input} ${outputDir} $1
$MG4J_NYU_SCRIPTS/experiments/clustering/run-queries-for-clusters.sh ${clusterDir} ${input} ${outputDir} $1

shift

done

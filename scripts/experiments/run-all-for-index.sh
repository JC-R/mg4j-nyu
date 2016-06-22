#!/bin/bash

if [ -z "${ROOT}" ]; then export ROOT=`readlink -f ../`; fi;
CLASSPATH=`find "${ROOT}/../target/" -name "*.jar" | paste -d: -s`

queryFile=$1
outputDir=$2
unorderedFullIndex=$3
unorderedClusterDir=$4

inputBase=`basename ${queryFile}`

# Run for unordered full index
java -Xmx3g -cp "${CLASSPATH}" edu.nyu.tandon.experiments.RunQueries \
    -i ${queryFile} \
    -t "${outputDir}/full/unordered/${inputBase}.time" \
    -r "${outputDir}/full/unordered/${inputBase}.top10" \
    -l "${outputDir}/full/unordered/${inputBase}.listlengths" \
    ${unorderedFullIndex}

# Run for unordered clusters
${ROOT}/experiments/clustering/run-queries-for-clusters.sh ${unorderedClusterDir} ${queryFile} "${outputDir}/clusters/unordered"
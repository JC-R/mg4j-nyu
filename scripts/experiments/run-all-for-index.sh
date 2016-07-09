#!/bin/bash

source "${MG4J_NYU_SCRIPTS}/commons.sh"

queryFile=$1
outputDir=$2
unorderedFullIndex=$3
unorderedClusterDir=$4
orderedFullIndex=$5
orderedClusterDir=$6
csi=$7

if [ -z "${queryFile}" ]; then echo "You have to define query file."; exit 1; fi;
if [ -z "${outputDir}" ]; then echo "You have to define output directory."; exit 1; fi;
if [ -z "${unorderedFullIndex}" ]; then echo "You have to define Unordered Full Index."; exit 1; fi;
if [ -z "${unorderedClusterDir}" ]; then echo "You have to define Unordered Cluster Dir."; exit 1; fi;
if [ -z "${orderedFullIndex}" ]; then echo "You have to define Ordered Full Index."; exit 1; fi;
if [ -z "${orderedClusterDir}" ]; then echo "You have to define Ordered Cluster Dir."; exit 1; fi;
if [ -z "${csi}" ]; then echo "You have to define Central Sample Index."; exit 1; fi;

inputBase=`basename ${queryFile}`

mkdir -p "${outputDir}/full"
mkdir -p "${outputDir}/full/unordered"
mkdir -p "${outputDir}/full/ordered"
mkdir -p "${outputDir}/clusters"
mkdir -p "${outputDir}/clusters/unordered"
mkdir -p "${outputDir}/clusters/ordered"

# Run for unordered full index
java -Xmx3g edu.nyu.tandon.experiments.ExtractFeatures \
    -i ${queryFile} \
    -t "${outputDir}/full/unordered/${inputBase}.time" \
    -r "${outputDir}/full/unordered/${inputBase}.top10" \
    -l "${outputDir}/full/unordered/${inputBase}.listlengths" \
    ${unorderedFullIndex}

# Run for ordered full index
java -Xmx3g edu.nyu.tandon.experiments.ExtractFeatures \
    -i ${queryFile} \
    -t "${outputDir}/full/ordered/${inputBase}.time" \
    -r "${outputDir}/full/ordered/${inputBase}.top10" \
    -l "${outputDir}/full/ordered/${inputBase}.listlengths" \
    ${orderedFullIndex}

# Run for unordered clusters
${MG4J_NYU_SCRIPTS}/experiments/clustering/run-queries-for-clusters.sh \
    ${unorderedClusterDir} \
    ${queryFile} \
    "${outputDir}/clusters/unordered" \
    ${csi}

# Run for ordered clusters
${MG4J_NYU_SCRIPTS}/experiments/clustering/run-queries-for-clusters.sh \
    ${orderedClusterDir} \
    ${queryFile} \
    "${outputDir}/clusters/ordered" \
    ${csi}
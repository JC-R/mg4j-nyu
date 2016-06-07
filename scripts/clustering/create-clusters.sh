#!/bin/bash

#
# create-clusters.sh
#
# Produce clusters of a global index based on files with titles.
#
# Arguments:
# 1) working directory;
# 2) global index base name (absolute path, e.g., /home/user/index/basename);
# 3) output index (and clusters) base name
# 4...) the list of files containing titles of the documents in clusters.
#

if [ -z "${ROOT}" ]; then export ROOT=`readlink -f ../`; fi;
CLASSPATH="${ROOT}/../target/artifacts/mg4j_nyu_jar/mg4j-nyu.jar"

workDir=$1
globalBase=$2
outputName=$3
shift
shift
shift

if [ -z "${workDir}" ]; then echo "You have to define working directory."; exit 1; fi;
if [ -z "${globalBase}" ]; then echo "You have to define the basename of a global index."; exit 1; fi;
if [ -z "${outputName}" ]; then echo "You have to define the output name."; exit 1; fi;

# Create the strategy
${ROOT}/clustering/cluster-mappings.sh ${workDir} "${globalBase}.titles" "$@"
clusterList=`find "${workDir}/numbers" -type f | sort | paste -sd "," -`
java -cp ${CLASSPATH} edu.nyu.tandon.index.cluster.SelectiveDocumentalIndexStrategy -a "-c:${clusterList}" "${workDir}/${outputName}.strategy"

# Create the clusters
java -cp ${CLASSPATH} it.unimi.di.big.mg4j.tool.PartitionDocumentally \
    -c POSITIONS:NONE \
    -s "${workDir}/${outputName}.strategy" \
    "${globalBase}-text" \
    "${workDir}/${outputName}"
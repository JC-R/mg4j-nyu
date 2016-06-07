#!/bin/bash

#
# create-clusters.sh
#
# Produce clusters of a global index based on files with titles.
#
# Arguments:
# 1)
#

CLASSPATH=../../target/artifacts/mg4j_nyu_jar/mg4j-nyu.jar

workDir=$1
globalBase=$2
shift
shift

if [ -z "${workDir}" ]; then echo "You have to define working directory."; exit 1; fi;
if [ -z "${globalBase}" ]; then echo "You have to define the basename of a global index."; exit 1; fi;

./cluster-mappings.sh ${workDir} "${globalBase}.titles" "$@"
clusterList=`find "${workDir}/numbers" -type f | sort | paste -sd "," -`
java -cp ${CLASSPATH} edu.nyu.tandon.index.cluster.SelectiveDocumentalIndexStrategy -a "-c:${clusterList}" "${workDir}/strategy"
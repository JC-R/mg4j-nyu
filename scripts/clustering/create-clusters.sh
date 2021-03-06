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
#

source "${MG4J_NYU_SCRIPTS}/commons.sh"

workDir=$1
globalBase=$2
outputName=$3

if [ -z "${workDir}" ]; then echo "You have to define working directory."; exit 1; fi;
if [ -z "${globalBase}" ]; then echo "You have to define the basename of a global index."; exit 1; fi;
if [ -z "${outputName}" ]; then echo "You have to define the output name."; exit 1; fi;

# Create the strategy
clusterList=`find "${workDir}/numbers" -type f | sort | paste -sd "," -`
totalNumberOfDocuments=`wc -l "${globalBase}.titles" | cut -d" " -f1`
java edu.nyu.tandon.index.cluster.SelectiveDocumentalIndexStrategy -a "-c:${clusterList}" -n ${totalNumberOfDocuments} "${workDir}/${outputName}.strategy"

# Create the clusters
java it.unimi.di.big.mg4j.tool.PartitionDocumentally \
    -c POSITIONS:NONE \
    -s "${workDir}/${outputName}.strategy" \
    "${globalBase}-text" \
    "${workDir}/${outputName}"

# Produce term mappings
ls ${workDir}/*-*terms | while read termFile;
do
        base=`basename ${termFile}`
        number=`echo ${base} | sed "s/.*-//" | sed "s/\..*//"`
        java it.unimi.dsi.sux4j.mph.MWHCFunction -s 32 "${workDir}/${outputName}-${number}.mwhc" "${workDir}/${outputName}-${number}.terms"
        java it.unimi.dsi.sux4j.util.SignedFunctionStringMap "${workDir}/${outputName}-${number}.mwhc" "${workDir}/${outputName}-${number}.termmap"
done

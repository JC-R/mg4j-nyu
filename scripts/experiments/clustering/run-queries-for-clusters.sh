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

if [ -z "${ROOT}" ]; then export ROOT=`readlink -f ../../`; fi;
CLASSPATH=`find "${ROOT}/../target/" -name "*.jar" | paste -d: -s`

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

java -Xmx3g -cp "${CLASSPATH}" edu.nyu.tandon.experiments.SelectShards \
    -i ${input} \
    -t "${outputDir}/${inputBase}.shards.time" \
    -r "${outputDir}/${inputBase}.shards.t10" \
    ${base} \
    ${csiBase}

ls ${dir}/*-*terms | while read file;
do
        clusterBase=`echo ${file} | sed "s/\.terms$//"`

        number=`basename ${file} | sed "s/.*-//" | sed "s/\..*//"`
        echo "${number}"

        mkdir -p "${outputDir}/${number}"

        java -Xmx3g -cp "${CLASSPATH}" edu.nyu.tandon.experiments.ExtractFeatures -g \
            -i ${input} \
            -t "${outputDir}/${number}/${inputBase}.time" \
            -r "${outputDir}/${number}/${inputBase}.top10" \
            -l "${outputDir}/${number}/${inputBase}.listlengths" \
            ${clusterBase}

        java -cp "${CLASSPATH}" edu.nyu.tandon.experiments.TranslateToGlobalIds \
            -i "${outputDir}/${number}/${inputBase}.top10" \
            -s "${base}.strategy" \
            -c ${number}
done

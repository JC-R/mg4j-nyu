#!/bin/bash

#
# run-queries-for-clusters.sh
#
# Run queries for all clusters in a directory.
#
# Arguments:
# 1) directory
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

java -Xmx3g -cp "${CLASSPATH}" edu.nyu.tandon.experiments.SelectShards \
    -i ${input} \
    -t "${outputDir}/shards.time" \
    -r "${outputDir}/shards.results" \
    "${dir}/`ls ${dir} | egrep '\.strategy' | sed 's/\.strategy//'`" \
    ${csiBase}

ls ${dir}/*-*properties | while read file;
do
        clusterBase=`echo ${file} | sed "s/\.properties$//"`
        number=`basename ${file} | sed "s/.*-//" | sed "s/\..*//"`
        echo "${number}"

        java -cp "${CLASSPATH}" edu.nyu.tandon.experiments.RunQueries -g \
            -i ${input} \
            -t "${outputDir}/${number}.time" \
            -r "${outputDir}/${number}.results" \
            ${clusterBase}
done

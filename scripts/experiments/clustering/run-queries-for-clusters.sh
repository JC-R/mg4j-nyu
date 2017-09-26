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
# 4) buckets
#

source "${MG4J_NYU_SCRIPTS}/commons.sh"

dir=$1
input=$2
outputDir=$3
buckets=$4
k=$5

if [ -z "${dir}" ]; then echo "You have to define cluster directory."; exit 1; fi;
if [ -z "${input}" ]; then echo "You have to define input file."; exit 1; fi;
if [ -z "${outputDir}" ]; then echo "You have to define output directory."; exit 1; fi;
if [ -z "${buckets}" ]; then echo "You have to define the number of buckets."; exit 1; fi;
if [ -z "${k}" ]; then k=100; fi;

inputBase=`basename ${input}`
base="${dir}/`ls ${dir} | egrep '\.strategy' | sed 's/\.strategy//'`"
strategy="${dir}/`ls ${dir} | egrep '\.strategy'`"

starttime=$(date +%s)

set -e

ls ${dir}/*-*terms | sort | while read file;
do
        clusterBase=`echo ${file} | sed "s/\.terms//"`

        number=`basename ${file} | sed "s/.*-//" | sed "s/\..*//"`
        echo "${number}"

        java edu.nyu.tandon.experiments.cluster.ExtractClusterFeatures -g \
            -i ${input} \
            -o "${outputDir}/${inputBase}" \
            -s ${number} \
            -k ${k} \
            -b ${buckets} \
            ${clusterBase}

done

endtime=$(date +%s)

echo "Extracting time: $((endtime-starttime))s"
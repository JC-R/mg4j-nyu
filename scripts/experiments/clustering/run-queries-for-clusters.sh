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

source "${MG4J_NYU_SCRIPTS}/commons.sh"

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

starttime=$(date +%s)

java -Xmx3g edu.nyu.tandon.experiments.cluster.ExtractShardScores \
    -s redde \
    -i ${input} \
    -r "${outputDir}/${inputBase}.redde.scores" \
    -c `ls ${dir}/*-*titles | wc -l` \
    ${base} \
    ${csiBase}

java -Xmx3g edu.nyu.tandon.experiments.cluster.ExtractShardScores \
    -s shrkc \
    -i ${input} \
    -r "${outputDir}/${inputBase}.shrkc.scores" \
    -c `ls ${dir}/*-*titles | wc -l` \
    ${base} \
    ${csiBase}

set -x
ls ${dir}/*-*titles | sort | while read file;
do
        clusterBase=`echo ${file} | sed "s/\.titles$//"`

        number=`basename ${file} | sed "s/.*-//" | sed "s/\..*//"`
        echo "${number}"

        mkdir -p "${outputDir}/${number}"

        mv "${outputDir}/${inputBase}.redde.scores.${number}" "${outputDir}/${number}/${inputBase}.redde.scores"
        mv "${outputDir}/${inputBase}.redde.scores.${number}" "${outputDir}/${number}/${inputBase}.shrkc.scores"

        java -Xmx4g edu.nyu.tandon.experiments.cluster.ExtractClusterFeatures -g \
            -i ${input} \
            -r "${outputDir}/${number}/${inputBase}.top10" \
            ${clusterBase}

        java -Xmx4g edu.nyu.tandon.ml.features.SegmentCounter \
            -i "${outputDir}/${number}/${inputBase}.top10" \
            --num-bins 10 \
            --num-docs `wc -l ${file} | cut -d" " -f1` \
            --column "results"
done

endtime=$(date +%s)

echo "Extracting time: $((endtime-starttime))s"
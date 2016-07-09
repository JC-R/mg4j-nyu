#!/bin/bash

#
# Translate results from local to global IDs
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

if [ -z "${dir}" ]; then echo "You have to define cluster directory."; exit 1; fi;
if [ -z "${input}" ]; then echo "You have to define input file."; exit 1; fi;
if [ -z "${outputDir}" ]; then echo "You have to define output directory."; exit 1; fi;

inputBase=`basename ${input}`
strategy="${dir}/`ls ${dir} | egrep '\.strategy'`"

ls ${dir}/*-*terms | while read file;
do
        clusterBase=`echo ${file} | sed "s/\.terms$//"`
        number=`basename ${file} | sed "s/.*-//" | sed "s/\..*//"`
        echo "${number}"

        java edu.nyu.tandon.experiments.TranslateToGlobalIds \
            -i "${outputDir}/${number}/${inputBase}.top10" \
            -s ${strategy} \
            -c ${number}
done

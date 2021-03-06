#!/bin/bash

#
# create-csi.sh
#
# Produce a central sample index, i.e., an index containing only a small (random) portion of documents.
#
# Arguments:
# 1) working directory;
# 2) global index base name (absolute path, e.g., /home/user/index/basename);
# 3) output index (and clusters) base name;
# 4) fraction of the original index to use for CSI.
#

source "${MG4J_NYU_SCRIPTS}/commons.sh"

workDir=$1
globalBase=$2
outputName=$3
fraction=$4

if [ -z "${workDir}" ]; then echo "You have to define working directory."; exit 1; fi;
if [ -z "${globalBase}" ]; then echo "You have to define the basename of a global index."; exit 1; fi;
if [ -z "${outputName}" ]; then echo "You have to define the output name."; exit 1; fi;
if [ -z "${fraction}" ]; then echo "You have to define the fraction."; exit 1; fi;

titles="${globalBase}.titles"
rm -fR "${workDir}/numbers"

# Calculate the size
size=`wc -l ${titles} | cut -d" " -f1`
csiSize=$(printf %.0f `echo "scale=1;${size} * ${fraction}" | bc`)

if [ "${csiSize}" -ge "${size}" ]; then echo "Whoa! Let's take a step back: CSI just has to be smaller than the global index."; exit 1; fi;

# Choose random titles
random=`mktemp`
seq 0 $((${size} - 1)) | sort -R > ${random}
mkdir -p "${workDir}/numbers"
cat ${random} | head "-n${csiSize}" | sort -n > "${workDir}/numbers/0"
cat ${random} | tail "-n$((${size} - ${csiSize}))" | sort -n > "${workDir}/numbers/1"

${MG4J_NYU_SCRIPTS}/clustering/create-clusters.sh ${workDir} ${globalBase} ${outputName}

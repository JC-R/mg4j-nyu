#!/bin/bash

#
# cluster-mappings.sh
#
# Produce ID mappings for multiple clusters (see the documentation of cluster-mapping.sh).
#
# Arguments:
# 1) working directory;
# 2) the global index's *.titles file;
# 3...) the list of files containing titles of the documents in clusters.
#

workDir=$1
global=$2
shift
shift

globalSorted=`mktemp`
seq 0 $((`wc -l < ${global}` - 1)) | paste -d" " ${global} - | sort > ${globalSorted}

>&2 echo "Creating clusters mappings for index '${global}'"
for cluster in "$@"
do
        base=`basename ${cluster}`
        >&2 echo "Creating cluster mapping for '${cluster}'"
        ./cluster-mapping.sh -s ${globalSorted} ${cluster} "${workDir}/numbers/${base}"
done
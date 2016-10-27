#!/bin/bash

#
# Arguments:
# 1) cluster directory
#

source "${MG4J_NYU_SCRIPTS}/commons.sh"

dir=$1

if [ -z "${dir}" ]; then echo "You have to define cluster directory (1)."; exit 1; fi;

ls ${dir}/*-*titles | while read file;
do
        number=`basename ${file} | sed "s/.*-//" | sed "s/\..*//"`
        maxId=`wc -l ${file} | awk {'print $1'}`
        echo "${number} ${maxId}"
done | sort -n -k1 | cut -d" " -f2


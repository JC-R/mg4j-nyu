#!/bin/bash

source "${MG4J_NYU_SCRIPTS}/commons.sh"

workDir=$1

if [ -z "${workDir}" ]; then echo "You have to define working directory."; exit 1; fi;

i=0
find "${workDir}/titles" -type f | sort | while read file;
do
        cp ${file} "${workDir}/${i}"
        i=$((i+1))
done

ls ${workDir}/*-*terms | while read termFile;
do
        base=`basename ${termFile}`
        number=`echo ${base} | sed "s/.*-//" | sed "s/\..*//"`
        outputName=`echo ${termFile} | sed "s/\.terms$//"`
        mv "${workDir}/${number}" "${outputName}.titles"
done
#!/bin/bash

if [ -z "${ROOT}" ]; then export ROOT=`readlink -f ../`; fi;
CLASSPATH=`find "${ROOT}/../target/" -name "*.jar" | paste -d: -s`

workDir=$1

if [ -z "${workDir}" ]; then echo "You have to define working directory."; exit 1; fi;

#cp "${workDir}/titles/${number}" "${workDir}/${outputName}-${number}.titles"

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
        mv "${workDir}/${number}" "${workDir}/${outputName}-${number}.titles"
done
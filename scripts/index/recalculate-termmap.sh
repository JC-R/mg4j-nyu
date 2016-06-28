#!/bin/bash

#
# recalculate-termmap.sh
#
# Recalculates termmap for all indices in the directory
#
# Arguments:
# 1) working directory
#

if [ -z "${ROOT}" ]; then export ROOT=`readlink -f ../`; fi;
CLASSPATH=`find "${ROOT}/../target/" -name "*.jar" | paste -d: -s`

dir=$1

if [ -z "${dir}" ]; then echo "You have to define working directory."; exit 1; fi;

# Produce term mappings
ls ${dir}/*-*terms | while read termFile;
do
        base=`basename ${termFile}`
        number=`echo ${base} | sed "s/.*-//" | sed "s/\..*//"`
        mwhcFile=`echo ${termFile} | sed "s/terms$/mwhc/"`
        termmapFile=`echo ${termFile} | sed "s/terms$/termmap/"`
        java -cp ${CLASSPATH} it.unimi.dsi.sux4j.mph.MWHCFunction -s 32 ${mwhcFile} ${termFile}
        java -cp ${CLASSPATH} it.unimi.dsi.sux4j.util.SignedFunctionStringMap ${mwhcFile} ${termmapFile}
done
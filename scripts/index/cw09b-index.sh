#!/bin/bash

if [[ $# -ne 2 ]]; then
    echo "$0: Two arguments are expected: <work-dir> <cw-location>."
    exit 4
fi

WORK_DIR=$1
CW_LOCATION=$2

set -e

export CLASSPATH=${MG4J_NYU_CLASSPATH}

find ${CW_LOCATION} -iname \*.gz -type f | \
    java it.unimi.di.big.mg4j.document.WarcDocumentSequence_NYU \
        -z -f it.unimi.di.big.mg4j.document.HtmlDocumentFactory -p encoding=UTF-8 \
        ${WORK_DIR}/cw09b.sequence

java it.unimi.di.big.mg4j.tool.IndexBuilder \
    -s 1000000 -S ${WORK_DIR}/cw09b.sequence \
    -t EnglishStemmer -I text -c POSITIONS:NONE \
    ${WORK_DIR}/cw09b > ~/out 2> ~/err

java it.unimi.dsi.sux4j.mph.MWHCFunction -s 32 ${WORK_DIR}/cw09b-text.mwhc ${WORK_DIR}/cw09b-text.terms
java it.unimi.dsi.sux4j.util.SignedFunctionStringMap ${WORK_DIR}/cw09b-text.mwhc ${WORK_DIR}/cw09b-text.termmap

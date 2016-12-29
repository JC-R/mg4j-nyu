#!/bin/bash

if [[ $# -ne 2 ]]; then
    echo "$0: Two arguments are expected: <work-dir> <gov2-location>."
    exit 4
fi

WORK_DIR=$1
GOV2_LOCATION=$2

set -e

export CLASSPATH=${MG4J_NYU_CLASSPATH}

find ${GOV2_LOCATION} -iname \*.gz -type f | \
    java it.unimi.di.big.mg4j.document.TRECDocumentCollection \
        -z -f it.unimi.di.big.mg4j.document.HtmlDocumentFactory -p encoding=UTF-8 \
        ${WORK_DIR}/gov2.sequence

java \
    it.unimi.di.big.mg4j.tool.IndexBuilder \
    -s 1000000 -S ${WORK_DIR}/gov2.sequence \
    -t EnglishStemmer -I text -c POSITIONS:NONE \
    ${WORK_DIR}/gov2 > ~/out 2> ~/err

java it.unimi.dsi.sux4j.mph.MWHCFunction -s 32 ${WORK_DIR}/gov2-text.mwhc ${WORK_DIR}/gov2-text.terms
java it.unimi.dsi.sux4j.util.SignedFunctionStringMap ${WORK_DIR}/gov2-text.mwhc ${WORK_DIR}/gov2-text.termmap

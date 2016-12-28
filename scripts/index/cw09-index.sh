#!/bin/bash

getopt --test > /dev/null
if [[ $? -ne 4 ]]; then
    echo "I’m sorry, `getopt --test` failed in this environment."
    exit 1
fi

SHORT=t:m
LONG=threads:memory
PARSED=`getopt --options $SHORT --longoptions $LONG --name "$0" -- "$@"`
if [[ $? -ne 0 ]]; then
    exit 2
fi
eval set -- "$PARSED"

threads=2
XMX="-Xmx4g"

while true; do
    case "$1" in
        -t|--threads)
            threads="$2"
            shift 2
            ;;
        -m|--memory)
            XMX="-Xmx$2"
            shift 2
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "Programming error: $1"
            exit 3
            ;;
    esac
done

# handle non-option arguments
if [[ $# -ne 2 ]]; then
    echo "$0: Two arguments are expected: <work-dir> <cw-location>."
    exit 4
fi

WORK_DIR=$1
CW_LOCATION=$2

set -e

export CLASSPATH=${MG4J_NYU_CLASSPATH}

starttime=$(date +%s)

# Parallel

rm -f ${WORK_DIR}/cw09.titles ${WORK_DIR}/cw09-text.* ${WORK_DIR}/cw09-split-* ${WORK_DIR}/split-*

TMP=$(mktemp)
find ${CW_LOCATION} -iname \*.gz -type f | sort >$TMP
split -n l/${threads} $TMP ${WORK_DIR}/split-

(for split in ${WORK_DIR}/split-*; do
(
    name=`basename ${split}`
	java ${XMX} -server \
		it.unimi.di.big.mg4j.document.WarcDocumentSequence_NYU \
		-z -f it.unimi.di.big.mg4j.document.HtmlDocumentFactory -p encoding=iso-8859-1 ${WORK_DIR}/cw09b-${name}.sequence $(cat $split)

	java ${XMX} -server -Dit.unimi.di.law.warc.io.version=false -Dit.unimi.di.law.warc.records.useburl=false it.unimi.di.big.mg4j.tool.Scan \
	    -s 1000000 -S ${WORK_DIR}/cw09b-${name}.sequence -t EnglishStemmer \
	    -I text -c COUNTS ${WORK_DIR}/cw09b-${name} > ${WORK_DIR}/${name}.out 2>${WORK_DIR}/${name}.err

)&

done

wait)

# Check that all instances have completed

if (( $(find ${WORK_DIR} -iname cw09-split-\*-text.cluster.properties | wc -l) != ${threads} )); then
	echo "ERROR: Some instance did not complete correctly" 1>&2
	exit 1
fi

java ${XMX} -server it.unimi.di.big.mg4j.tool.Concatenate -c POSITIONS:NONE ${WORK_DIR}/cw09-text \
	$(find ${WORK_DIR} -iname cw09-split-\*-text@\*.sizes | sort | sed s/.sizes//)
cat $(find ${WORK_DIR} -iname cw09-split-\*.titles | sort) > ${WORK_DIR}/cw09.titles

java ${XMX} -server it.unimi.dsi.sux4j.mph.MWHCFunction -s 32 ${WORK_DIR}/cw09-text.mwhc ${WORK_DIR}/cw09-text.terms

java ${XMX} -server it.unimi.dsi.sux4j.util.SignedFunctionStringMap ${WORK_DIR}/cw09-text.mwhc ${WORK_DIR}/cw09-text.termmap


endtime=$(date +%s)

echo "Indexing time: $((endtime-starttime))s"



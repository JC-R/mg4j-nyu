#!/bin/bash
set -e

# Script to generate raw docHits
# ------------------------------

version=5.4.1

WORK_DIR=/home/juan/work/IR/ClueWeb09b

export CLASSPATH=.:/home/juan/sandbox/mg4j-nyu/*:/home/juan/sandbox/mg4j-nyu/lib/*

starttime=$(date +%s)

java -Xmx58g edu.nyu.tandon.experiments.RawDocHits \
-r 10000 -I 100M-s.txt -d $WORK_DIR/docHits-raw.txt $WORK_DIR/index/qs-xdoc/cw09b-text?inmemory=1






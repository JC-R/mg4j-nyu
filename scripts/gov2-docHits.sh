#!/bin/bash
set -e

# Script to generate raw docHits
# ------------------------------

WORK_DIR=~/work/data/experiments/
INDEX_DIR=~/work/data/IR/Gov2/index/mg4j/qs-xdoc/

pwd=`pwd`
cd $WORK_DIR

export CLASSPATH=.:~/work/sandbox/mg4j-nyu/*

starttime=$(date +%s)

# $1 = queries
# $2 = output file

for s in AND OR; do
java -Xmx32g edu.nyu.tandon.tool.RawDocHits -r 1280 -I queries/100K.$s.train -d tmp/gov2-100K-dh-$s.txt $INDEX_DIR/gov2-text?inmemory=1 &
done

cd $pwd



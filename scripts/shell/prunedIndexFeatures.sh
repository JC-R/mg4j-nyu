#!/bin/bash
#set -e

# Runs the MG4J k-out-of-n queries and performs evaluation
export CLASSPATH=/home/juan/work/code/mg4j-nyu/*
export PATH=.:$PATH

CORPUS=gov2
RESULTS=results

OPTIONS="-server -Dlogback.configurationFile=logback.xml"
INDEX_DIR=/home/juan/work/data/IR/Gov2/index/mg4j/
BASELINE_INDEX=$INDEX_DIR/qs-xdoc/$CORPUS-text
PRUNED_DIR=$INDEX_DIR/pruned
WORK_DIR=/home/juan/work/data/experiments
PWD=`pwd`
cd $WORK_DIR

rm -f $RESULTS/*.features.*

# prune levels
#for n in 01 02 03 04 05 10 15 20 25 30; do
for n in 01 02 03 04 05 10 15 20; do
for k in top10 top1k; do
for s in AND OR; do

#  requires pruned index
# ----------------------
INDEX=$PRUNED_DIR/$CORPUS-$k-$n
fname=$CORPUS-$k-$n

# for index independent features, use flags: --trec -T $INDEX_DIR/qs-xdoc/$CORPUS.titles
# for term features (length,doc length, BM25) use: --verbose
flags =

# get results
java -Xmx4g $OPTIONS edu.nyu.tandon.tool.PrunedIndexFeatures $flags -d $RESULTS/$fname-features.csv $INDEX-0  2>$RESULTS/$fname-features.log

done
done
done

cd $PWD


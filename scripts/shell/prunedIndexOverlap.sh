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

rm -f $RESULTS/*.overlap.*

cat <(cat lists/$CORPUS.topics.AND.txt) >lists/$CORPUS.trec_eval.AND.txt
cat <(cat lists/$CORPUS.topics.OR.txt) >lists/$CORPUS.trec_eval.OR.txt

# prune levels
for n in 01 02 03 04 05 10 15 20; do
for k in top10 top1k; do
for s in AND OR; do

INDEX=$PRUNED_DIR/$CORPUS-$k-$n
fname=$CORPUS-$k-$n-$s

# for index independent features, use flags: --trec
flags=-T $INDEX_DIR/qs-xdoc/$CORPUS.titles -t $INDEX.titles -I lists/$CORPUS.trec_eval.$s.txt -i $INDEX-0

# top 10
java -Xmx10g $OPTIONS edu.nyu.tandon.experiments.PrunedIndexOverlap $flags -r 10 -d $RESULTS/$fname-overlap-10.txt $BASELINE_INDEX >$RESULTS/$fname-overlap-10.log 2>$RESULTS/$fname-overlap-10.err &

# top 1K
java -Xmx10g $OPTIONS edu.nyu.tandon.experiments.PrunedIndexOverlap $flags -r 1000 -d $RESULTS/$fname-overlap-1k.txt $BASELINE_INDEX >$RESULTS/$fname-overlap-1k.log 2>$RESULTS/$fname-overlap-1k.err &

done
done
wait
done

cd $PWD


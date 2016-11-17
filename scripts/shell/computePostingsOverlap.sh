#!/usr/bin/env bash

CORPUS=gov2
WORK_DIR=/home/juan/work/data/experiments
RESULTS=results
PROGRAM=/home/juan/work/code/mg4j-nyu/scripts/ruby
PWD=`pwd`
cd $WORK_DIR

# prune levels
for n in 01 02 03 04 05 10 15 20; do
for k in top10 top1k; do
for s in AND OR; do
for topk in 10 1000; do
for scoring in global; do

# compute overlap
fname=$CORPUS-$k-$n-$s-$scoring
ruby $PROGRAM/extractPostingsOverlap.rb $RESULTS/$CORPUS-baseline-$s-.postings.txt $RESULTS/$fname.1k.postings.txt $topk $CORPUS $k $n $s $scoring

done
done
done
done
done

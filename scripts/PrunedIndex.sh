#!/bin/bash
#
# Juan - jcr365@nyu.edu
#
# Use Partitioning classes to generate a pruned index.
# A pruned index is a full index that has been partitioned into 2 disjoint indeces.
# Index 0 is the pruned index; Index 1 is the rest of the localPostings, Index 1 is not physically manifested.
#
# Requires: PostingPruningStrategies must exist in serialized format
#
#
#
export CLASSPATH=$CLASSPATH:/home/juan/sandbox/mg4j-nyu/mg4j-nyu.jar:/home/juan/work/runtime/*

CORPUS=gov2
INDECES=/home/juan/work/IR/Gov2/index/mg4j
FULL_INDEX=$INDECES/qs-xdoc/$CORPUS-text
INDEX_DIR=$INDECES/pruned

export _JAVA_OPTIONS="-Xmx10g -XX:-UseConcMarkSweepGC"

for k in top10 top1k; do
for n in 01 02 03 04; do
~/sandbox/mg4j-nyu/scripts/SingletonPrunedIndex.sh $INDEX_DIR/prune-$CORPUS-$k-$n.strategy $FULL_INDEX $INDEX_DIR/$CORPUS-$k-$n &
done
wait
done

export _JAVA_OPTIONS="-Xmx30g -XX:-UseConcMarkSweepGC"
for n in 05 10 15 20 25 30; do
for k in top10 top1k; do
~/sandbox/mg4j-nyu/scripts/SingletonPrunedIndex.sh $INDEX_DIR/prune-$CORPUS-$k-$n.strategy $FULL_INDEX $INDEX_DIR/$CORPUS-$k-$n &
done
wait
done


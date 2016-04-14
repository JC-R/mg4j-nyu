#!/bin/bash
# Juan - NYU
#
#  Create any number of pruning strategies for later use during pruning
#  PruningStretegy classes perform the work of selecting whther a posting is in or out
#

export CLASSPATH=$CLASSPATH:/home/juan/sandbox/mg4j-nyu/mg4j-nyu.jar:/home/juan/work/runtime/*
export _JAVA_OPTIONS="-Xmx54g -XX:-UseConcMarkSweepGC"

CORPUS=gov2
POSTING_ORDERING_DIR=/home/juan/drf
OUTPUT_DIR=/home/juan/work/IR/Gov2/index/mg4j/pruned/
FULL_INDEX=/home/juan/work/IR/Gov2/index/mg4j/qs-xdoc/gov2-text
TITLES=/home/juan/work/IR/Gov2/index/mg4j/qs-xdoc/gov2.titles

# Parameter -t = prunning ratio; can specify multiple times to generate multiple pruned indeces
# PRUNE_LEVELS="-t 0.01 -t 0.02 -t 0.03 -t 0.04 -t 0.05 -t 0.1 -t 0.15 -t 0.2 -t 0.25 -t 0.3 -t 0.35 -t 0.4"
PRUNE_LEVELS="-t 0.01 -t 0.02 -t 0.03 -t 0.04 -t 0.05 -t 0.1 -t 0.15 -t 0.2"

for c in top10 top1k
do
java edu.nyu.tandon.index.cluster.PostingPruningStrategy $PRUNE_LEVELS $FULL_INDEX $POSTING_ORDERING_DIR/$CORPUS-$c $OUTPUT_DIR/prune-$CORPUS-$c $TITLES $OUTPUT_DIR/$CORPUS-$c
done

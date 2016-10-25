#!/bin/bash
# Juan - NYU
#
#  Create any number of pruning strategies for later use during pruning
#  PruningStretegy classes perform the work of selecting whrther a posting is in or out
#
# input: $1    // an ordered list of postings, best first (descending) that will be used as prune criteria
#        $2    // output titles

CP=/home/juan/work/sandbox/mg4j-nyu/mg4j-nyu.jar
export _JAVA_OPTIONS="-Xmx54g -XX:-UseConcMarkSweepGC"

CORPUS=gov2
OUTPUT_DIR=/home/juan/work/data/IR/Gov2/index/mg4j/pruned/
FULL_INDEX=/home/juan/work/data/IR/Gov2/index/mg4j/qs-xdoc/gov2-text
TITLES=/home/juan/work/data/IR/Gov2/index/mg4j/qs-xdoc/gov2.titles

# Parameter -t = prunning ratio; can specify multiple times to generate multiple pruned indeces
#PRUNE_LEVELS="-t 0.01 -t 0.02 -t 0.03 -t 0.04 -t 0.05 -t 0.1 -t 0.15 -t 0.2 -t 0.25 -t 0.3 -t 0.35 -t 0.4 -t 0.5"
PRUNE_LEVELS="-t 0.01 -t 0.02 -t 0.03 -t 0.04 -t 0.05 -t 0.1 -t 0.15 -t 0.2 -t 0.25 -t 0.3 -t 0.35"

java -cp $CP edu.nyu.tandon.index.cluster.PostingPruningStrategy $PRUNE_LEVELS $FULL_INDEX $1 $OUTPUT_DIR/prune-$2 $TITLES $OUTPUT_DIR/$2

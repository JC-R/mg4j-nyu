#!/bin/bash
# Juan - NYU
#
#  Create any number of posting pruning strategies for use during actual pruning
#  PruningStretegy classes perform the work of selecting whether a posting is in or out
#
# input: $1    // an ordered list of postings, best first (descending) that will be used as prune criteria
#              format: termID,docID,... the rest is ignored
#        $2    corpus
#        $3    corpus main directory
#        $4    output name

CP=/home/juan/work/code/mg4j-nyu/mg4j-nyu.jar
export _JAVA_OPTIONS="-Xmx60g -XX:-UseConcMarkSweepGC -Dlogback.configurationFile=~/logback.xml"

# Parameter -t = prunning ratio; can specify multiple times to generate multiple pruned indeces
PRUNE_LEVELS="-t 0.01 -t 0.02 -t 0.03 -t 0.04 -t 0.05 -t 0.1 -t 0.15 -t 0.2 -t 0.25 -t 0.3 -t 0.35"

CORPUS=$2
HOME_DIR=$3

FULL_INDEX=$HOME_DIR/index/mg4j/qs-xdoc/$CORPUS-text
TITLES=$HOME_DIR/index/mg4j/qs-xdoc/$CORPUS.titles
OUTPUT_DIR=$HOME_DIR/index/mg4j/pruned/

java -cp $CP edu.nyu.tandon.index.cluster.PostingPruningStrategy $PRUNE_LEVELS -p $1 -T $TITLES -s $OUTPUT_DIR/$CORPUS-$4 $FULL_INDEX

#CORPUS=cw09b
#HOME_DIR=/home/juan/work/data/IR/ClueWeb09b
#
#FULL_INDEX=$HOME_DIR/index/mg4j/qs-xdoc/$CORPUS-text
#TITLES=$HOME_DIR/index/mg4j/qs-xdoc/$CORPUS.titles
#OUTPUT_DIR=$HOME_DIR/index/mg4j/pruned/
#
#java -cp $CP edu.nyu.tandon.index.cluster.PostingPruningStrategy $PRUNE_LEVELS -p $1 -T $TITLES -s $OUTPUT_DIR/$CORPUS-$2 $FULL_INDEX

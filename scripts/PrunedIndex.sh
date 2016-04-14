#!/bin/bash
#
# Juan - NYU
#
# Use the builtin Clustering/Partitioning libraries to generate a pruned index.
# A pruned index is a full index that has been partitioned into 2 disjoint indeces. Index 0 is the pruned index; Index 1 is the rest of the localPostings.
# Requires: PruningStrategies must exist
#
# Note that for purposes of pruning, Index 1 is not manifested physically 
#

export CLASSPATH=$CLASSPATH:/home/juan/sandbox/mg4j-nyu/mg4j-nyu.jar:/home/juan/work/runtime/*
export _JAVA_OPTIONS="-Xmx32g -XX:-UseConcMarkSweepGC"

CORPUS=gov2
INDECES=/home/juan/work/IR/Gov2/index/mg4j
FULL_INDEX=$INDECES/qs-xdoc/$CORPUS-text
INDEX_DIR=$INDECES/pruned

#for n in 01 02 03 04 05 10 15 20 25 30 35;
for n in 01 02 03 04 05 10 15;
do

# create the pruned index
for k in top10 top1k;
do
java edu.nyu.tandon.tool.PrunedPartition -s $INDEX_DIR/prune-$CORPUS-$k-$n.strategy $FULL_INDEX $INDEX_DIR/$CORPUS-$k-$n > log-$k-$n.log &
done
wait

# create string hash mappings for the pruned terms

for k in top10 top1k;
do
java -Xmx6096M -server it.unimi.dsi.sux4j.mph.MWHCFunction -s 32 $INDEX_DIR/$CORPUS-$k-$n-0.mwhc $INDEX_DIR/$CORPUS-$k-$n-0.terms
java -Xmx6096M -server it.unimi.dsi.sux4j.util.SignedFunctionStringMap $INDEX_DIR/$CORPUS-$k-$n-0.mwhc $INDEX_DIR/$CORPUS-$k$n-0.termmap &
done

done


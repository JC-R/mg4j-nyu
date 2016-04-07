#!/bin/bash
#
# Juan - NYU
#
# Use the builtin Clustering/Partitioning libraries to generate a pruned index.
# A pruned index is a full index that has been partitioned into 2 disjoint indeces. Index 0 is the pruned index; Index 1 is the rest of the localPostings.
# Requires: PruningStrategies must exist
#
# Note that for purposes of pruning, Index 1 is not needed; though it coould be used as a tier in other use cases
#

export CLASSPATH=$CLASSPATH:/home/juan/sandbox/mg4j-nyu/jar/mg4j-nyu.jar:/home/juan/work/runtime/*
#export _JAVA_OPTIONS="-Xmx32g -XX:-UseConcMarkSweepGC"

CORPUS=gov2
STRATEGY_DIR=/home/juan/drf
FULL_INDEX=/home/juan/work/IR/Gov2/index/mg4j/qs-xdoc/gov2-text
PRUNED_INDEX_DIR=/home/juan/work/IR/Gov2/index/mg4j/pruned

for n in {01,02,03,04,05,10,15,20,25,30,35,40,50};
do

# create the pruned index
for cut in {top10,top1k};
do
java edu.nyu.tandon.tool.PrunedPartition -s $STRATEGY_DIR/prune-$CORPUS-$c.$n.strategy $FULL_INDEX $PRUNED_INDEX_DIR/$CORPUS-$cut-$n > log-$cut-$n.log &
done
wait

# create string hash mappings for the pruned terms
for part in {0,1}
do
for cut in {top10,top1k};
do
java -Xmx6096M -server it.unimi.dsi.sux4j.mph.MWHCFunction -s 32 $CORPUS-$cut-$n-$part.mwhc $PRUNED_INDEX_DIR/$CORPUS-$cut-$n-$part.terms
java -Xmx6096M -server it.unimi.dsi.sux4j.util.SignedFunctionStringMap $CORPUS-$cut-$n-$part.mwhc $PRUNED_INDEX_DIR/$CORPUS-$cut-$n-$part.termmap &
done
done

done


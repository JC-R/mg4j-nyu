#!/usr/bin/env bash

export CLASSPATH=/home/juan/code/mg4j-nyu/target/*

export PROGRAM=~/code/mg4j-nyu/scripts/staticprune/SingletonIndexPrune.sh 
# cd /mnt/ssd

# create a model externally

# predict (in cluster)

# create selection strategy
# code/mg4j-nyu/scripts/staticprune/PostingPruningStrategy.sh /mnt/scratch/drf_top10_all_predict cw09b /mnt/ssd drf.top10.all

mv $3/strategy/*.titles $4/

# prune at different levels
for k in top1k; do

export _JAVA_OPTIONS=-Xmx10g
for t in 01 02 03 04 05; do
$PROGRAM $3/strategy/$1.$k-$t $2 $4/$1.$k-$t &
done
wait


export _JAVA_OPTIONS=-Xmx20g
for t in 10 15 20; do
$PROGRAM $3/strategy/$1.$k-$t $2 $4/$1.$k-$t &
done
wait

export _JAVA_OPTIONS=-Xmx30g
for t in 25 30; do
$PROGRAM $3/strategy/$1.$k-$t $2 $4/$1.$k-$t &
done
wait

done

# get posting lists
for m in all dochits doc term; do 
for n in 01 02 03 04 05 10 15 20 25 30; do 
 java edu.nyu.tandon.tool.IndexFeatures $m.top1k-$n-0 | cut -d ',' -f 1,2 > $m.top1k-$n.features & 
done
done

# make baseline results
#~/code/mg4j-nyu/scripts/staticprune/eval-queries/makebaseline-cw09b.sh

# pruned results
# ~/code/mg4j-nyu/scripts/staticprune/eval-queries/makeEvalfiles-cw09b.sh

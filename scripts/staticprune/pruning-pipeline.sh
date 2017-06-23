#!/usr/bin/env bash

export CLASSPATH=/home/juan/code/mg4j-nyu/target/*

cd /mnt/ssd

# create a model externally

# predict (in cluster)

# create selection strategy
code/mg4j-nyu/scripts/staticprune/PostingPruningStrategy.sh /mnt/scratch/drf_top10_all_predict cw09b /mnt/ssd drf.top10.all

# prune at different levels
for t in 01 02 03 04 05 10 15 20 25; do
code/mg4j-nyu/scripts/staticprune/SingletonIndexPrune.sh /mnt/ssd/strategy/cw09b-drf.top10.all-$t /mnt/ssd/qs-xdoc/cw09b-text
done

# make baseline results
#~/code/mg4j-nyu/scripts/staticprune/eval-queries/makebaseline-cw09b.sh

# pruned results
~/code/mg4j-nyu/scripts/staticprune/eval-queries/makeEvalfiles-cw09b.sh






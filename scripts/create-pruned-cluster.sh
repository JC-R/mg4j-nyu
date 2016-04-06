#!/usr/bin/env bash

CLASSPATH=/home/juan/work/sandbox/mg4j-nyu/*.jar:/home/juan/work/runtime/*
CLUSTER_DIR=/home/juan/work/IR/Gov2/index/mg4j/cluster
INDEX=gov2
currd=`pwd`
cd $CLUSTER_DIR

for c in {0,1}; do
for n in {01,02,03,04,05,10,20,25,30,40}; do
for k in {top10,top1k}; do

# create a string hash mapping for the pruned terms
java -Xmx6096M -server it.unimi.dsi.sux4j.mph.MWHCFunction -s 32 $INDEX-$k-$n-$c.mwhc $INDEX-$k-$n-$c.terms
java -Xmx6096M -server it.unimi.dsi.sux4j.util.SignedFunctionStringMap $INDEX-$k-$n-$c.mwhc $INDEX-$k-$n-$c.termmap

done
done
done
cd $currd

#!/usr/bin/env bash
#
# Juan jcr365@nyu.edu
#
# $1 = strategy and output naming
# $2 = full index (input)

# pruned index
#  parameters -s strategy file
#             -o output prefix
#             base index
java -server edu.nyu.tandon.index.prune.Partition2 -l 3000 -s $1.strategy -o $3 $2

# remap local/global docs and terms
java -Xmx6G -server it.unimi.dsi.sux4j.mph.MWHCFunction -s 32 $3-0.mwhc $3-0.terms
java -Xmx6G -server it.unimi.dsi.sux4j.util.SignedFunctionStringMap $3-0.mwhc $3-0.termmap


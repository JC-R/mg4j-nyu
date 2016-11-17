#!/usr/bin/env bash
#
# Juan jcr365@nyu.edu
#
# $1 = strategy and output naming
# $2 = full index (input)

# pruned index
java -server edu.nyu.tandon.tool.PrunedPartition --documentPruning -s $1.strategy -o $1 $2

# remap terms
java -Xmx6G -server it.unimi.dsi.sux4j.mph.MWHCFunction -s 32 $1-0.mwhc $1-0.terms
java -Xmx6G -server it.unimi.dsi.sux4j.util.SignedFunctionStringMap $1-0.mwhc $1-0.termmap

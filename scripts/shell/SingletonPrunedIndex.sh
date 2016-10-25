#!/usr/bin/env bash
#
# Juan jcr365@nyu.edu
#
# $1 = strategy (input)
# $2 = full index (input)
# $3 = pruned index basename (output)

# pruned index
java edu.nyu.tandon.tool.PrunedPartition -s $1 $2 $3 > $3.log

# term mappings for the pruned terms
java -Xmx6096M -server it.unimi.dsi.sux4j.mph.MWHCFunction -s 32 $3-0.mwhc $3-0.terms
java -Xmx6096M -server it.unimi.dsi.sux4j.util.SignedFunctionStringMap $3-0.mwhc $3-0.termmap

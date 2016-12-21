#!/bin/bash

#
# Compute statistical representations of all shards and entire index.
#
# Arguments:
# 1) basename of the cluster
#

basename=$1
if [ -z "${basename}" ]; then echo "You have to define cluster basename (1)."; exit 1; fi;

ls "${basename}*-*titles" | while read file;
do
        number=`basename ${file} | sed "s/.*-//" | sed "s/\..*//"`
        java -cp "${MG4J_NYU_CLASSPATH}" edu.nyu.tandon.shard.ranking.taily.StatisticalShardRepresentation "${basename}-${number}"
done

java -cp "${MG4J_NYU_CLASSPATH}" edu.nyu.tandon.shard.ranking.taily.StatisticalShardRepresentation "${basename}"

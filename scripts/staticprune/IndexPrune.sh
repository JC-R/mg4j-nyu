#!/bin/bash
#
# Juan - jcr365@nyu.edu
#
# Use Partitioning classes to generate a pruned index.
# A pruned index is a full index that has been partitioned into 2 disjoint indeces.
# Index 0 is the pruned index; Index 1 is the rest of the postings, Index 1 is not physically manifested.
#
# Requires: PostingPruningStrategies in serialized format (see PostingPruningStrategy class)
# A strategy is a POJO serialized to disk; that implements the strategy interface
#
#
# Parameters
#
#  CORPUS
#  STRATEGY_DIR
#  BASELINE_INDEX
#  MODEL NAME
#
if [ "$#" -ne 4 ]; then
    echo "Use: IndexPrune.sh <CORPUS> <STRATEGY_DIR> <BASELINE_INDEX> <MODEL>"
    exit 1
fi

CWD=`pwd`
MY_PATH="`dirname \"$0\"`"              # relative
MY_PATH="`( cd \"$MY_PATH\" && pwd )`"  # absolutized and normalized
if [ -z "$MY_PATH" ] ; then
  # error; for some reason, the path is not accessible
  # to the script (e.g. permissions re-evaled after suid)
  exit 1  # fail
fi
cd $CWD

CORPUS=$1
STRATEGY_DIR=$2
FULL_INDEX=$3
MODEL=$4

export CLASSPATH=/home/juan/code/mg4j-nyu/target/*

#export _JAVA_OPTIONS="-Xmx10g -XX:-UseConcMarkSweepGC -Dlogback.configurationFile=logback.xml"

export _JAVA_OPTIONS="-Xmx10g -Dlogback.configurationFile=logback.xml"

# these can be done all at once on a 64Gb RAM machine
for n in 01 02 03 04 05; do
fname=$CORPUS.$MODEL-$n
$MY_PATH/SingletonIndexPrune.sh $STRATEGY_DIR/$fname $FULL_INDEX &
done
wait

# do these one by one (if not enough RAM)
export _JAVA_OPTIONS="-Xmx60g -Dlogback.configurationFile=logback.xml"

for n in 10 15 20; do
fname=$CORPUS.$MODEL-$n
$MY_PATH/SingletonIndexPrune.sh $STRATEGY_DIR/$fname $FULL_INDEX?inmemory=true
done


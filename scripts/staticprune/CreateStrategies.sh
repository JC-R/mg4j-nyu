#!/usr/bin/env bash

export CLASSPATH=/home/juan/code/mg4j-nyu/target/*
export _JAVA_OPTIONS="-Xmx62g -Dlogback.configurationFile=~/logback.xml"
# export _JAVA_OPTIONS="-Xmx60g -XX:-UseConcMarkSweepGC -Dlogback.configurationFile=~/logback.xml"

CORPUS=$1
DATA_DIR=$2
INPUT=$3
OUT_DIR=$4

# Parameter -t = prunning ratio; can specify multiple times to generate multiple pruned indeces
# PRUNE_LEVELS="-t 0.01 -t 0.02 -t 0.03 -t 0.04 -t 0.05 -t 0.1 -t 0.15 -t 0.2 -t 0.25 -t 0.3 -t 0.4"
PRUNE_LEVELS="-t 0.3"

java edu.nyu.tandon.index.prune.PostingStrategy2 $PRUNE_LEVELS --parquet -p $DATA_DIR/$INPUT.parquet -s $OUT_DIR/strategy/$INPUT -T qs-xdoc/$CORPUS.titles qs-xdoc/$CORPUS-text



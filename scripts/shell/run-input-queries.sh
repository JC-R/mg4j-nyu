#!/usr/bin/env bash

# params:
#  1 - input queries
#  2 - flag e.g. --trec
#
#  last param - index

export CLASSPATH=/home/juan/sandbox/mg4j-nyu/*:/home/juan/work/runtime/*
NUM_RESULTS=1000

java -Dlogback.configurationFile=logback.xml edu.nyu.tandon.experiments.PrunedQuery -r $NUM_RESULTS -I $1 $2 $3 $4 $5 $6

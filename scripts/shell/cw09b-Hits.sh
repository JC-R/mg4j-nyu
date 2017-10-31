#!/bin/bash
#set -e

# $1 = method (RawHits = raw posting ID, rank) (BinnedRawHits = binned raw hits)
# $2 = queries
# $3 = dump filename
# $4 = dump every $4 queries
# $5 = dump every $5 postings
# $6 ?inmemory=1

WORK_DIR=/home/juan/work/research/data/cw09b
export CLASSPATH=/home/juan/code/mg4j-nyu/target/*

#java -Xmx32g -DlogbackConfigurationFile=~/logback.xml edu.nyu.tandon.experiments.$1 -r 1000 -I $2 -d $3 -D $4 $WORK_DIR/index/mg4j/qs-xdoc/cw09b-text?inmemory=1
java -Xmx30g -DlogbackConfigurationFile=~/logback.xml edu.nyu.tandon.experiments.$1 -r 1000 -I $2 -d $3 -D $4 -S $5 $WORK_DIR/index/mg4j/qs-xdoc/cw09b-text$6



#!/bin/bash
#set -e

# $1 = method (RawHits = raw posting ID, rank) (BinnedRawHits = binned raw hits)
# $2 = queries
# $3 = dump filename
# $4 = dump every $3 queries
#

WORK_DIR=/home/juan/work/data/IR/ClueWeb09b/
export CLASSPATH=/home/juan/work/code/mg4j-nyu/target/*

java -Xmx32g -DlogbackConfigurationFile=~/logback.xml edu.nyu.tandon.tool.$1 -r 1000 -I $2 -d $3 -D $4 $WORK_DIR/index/mg4j/qs-xdoc/cw09b-text?inmemory=1



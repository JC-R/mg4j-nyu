#!/bin/bash
#set -e

#
# $1 = queries
# $2 = dump filename
# $3 = dump every $3 queries
#

WORK_DIR=/home/juan/work/IR/Gov2
export CLASSPATH=.:/home/juan/sandbox/mg4j-nyu/*:/home/juan/work/runtime/*

java -Xmx16g -DlogbackConfigurationFile=~/logback.xml edu.nyu.tandon.tool.RawHits -r 1280 -I $1 -d $2 -D $3 $WORK_DIR/index/mg4j/qs-xdoc/gov2-text



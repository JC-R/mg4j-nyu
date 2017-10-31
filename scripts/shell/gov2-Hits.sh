#!/bin/bash
#set -e

#
# $1 = queries
# $2 = dump filename
# $3 = dump every $3 queries
#

WORK_DIR=~/work/data/IR/Gov2
export CLASSPATH=~/work/code/mg4j-nyu/target/*

java -Xmx16g -DlogbackConfigurationFile=~/logback.xml edu.nyu.tandon.tool.RawHits -r 1280 -I $1 -d $2 -D $3 $WORK_DIR/index/mg4j/qs-xdoc/gov2-text



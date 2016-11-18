#!/bin/bash
set -e

#sudo apt-add-repository -y ppa:webupd8team/java
#sudo apt-get -y update
#sudo apt-get -y install oracle-java8-installer
#sudo apt-get -y install ruby

version=5.4.1

#source ../common.sh

WORK_DIR=/home/juan/work/IR/Gov2/index/qs-xdoc
GOV2_LOCATION=/home/juan/media/Gov2/data

#if [[ ! -f mg4j-big-$version-bin.tar.gz ||  ! -f mg4j-big-deps.tar.gz ]]; then
#	curl http://mg4j.di.unimi.it/mg4j-big-$version-bin.tar.gz >mg4j-big-$version-bin.tar.gz
#	curl http://mg4j.di.unimi.it/mg4j-big-deps.tar.gz >mg4j-big-deps.tar.gz
#fi

#tar -zxvf mg4j-big-$version-bin.tar.gz
#tar -zxvf mg4j-big-deps.tar.gz

export CLASSPATH=.:/home/juan/sandbox/mg4j-nyu/*:/home/juan/work/runtime/*

starttime=$(date +%s)

# Parallel

rm -f $WORK_DIR/gov2.titles $WORK_DIR/gov2-text.* $WORK_DIR/gov2-split-* split-*

TMP=$(mktemp)
find $GOV2_LOCATION -type f -iname \*.gz | sort >$TMP
split -n l/8 $TMP split-

(for split in split-*; do
(
	java -Xmx6024M -server \
		it.unimi.di.big.mg4j.document.TRECDocumentCollection \
			-z -f  it.unimi.di.big.mg4j.document.HtmlDocumentFactory -p encoding=iso-8859-1 $WORK_DIR/gov2-$split.sequence $(cat $split)

	java -Xmx6024M -server edu.nyu.tandon.tool.XDOCScan \
	-s 1000000 -S $WORK_DIR/gov2-$split.sequence --downcase --language-model /home/juan/work/IR/Gov2/gov2.arpa \
	-I text -c COUNTS $WORK_DIR/gov2-$split >$WORK_DIR/$split.out 2>$WORK_DIR/$split.err
)& 

done

wait)

# Check that all instances have completed

if (( $(find $WORK_DIR -iname gov2-split-\*-text.cluster.properties | wc -l) != 8 )); then
	echo "ERROR: Some instance did not complete correctly" 1>&2
	exit 1
fi

java -Xmx7512M -server it.unimi.di.big.mg4j.tool.Concatenate -c POSITIONS:NONE $WORK_DIR/gov2-text \
	$(find $WORK_DIR -iname gov2-split-\*-text@\*.sizes | sort | sed s/.sizes//)
cat $(find $WORK_DIR -iname gov2-split-\*.titles | sort) >$WORK_DIR/gov2.titles

java -Xmx7512M -server edu.nyu.tandon.tool.XDOCConcatenate $(find $WORK_DIR -iname gov2-split-\*-text@\*.xdoc | sort) $WORK_DIR/gov2-text.xdoc

java -Xmx7512M -server it.unimi.dsi.sux4j.mph.MWHCFunction -s 32 $WORK_DIR/gov2-text.mwhc $WORK_DIR/gov2-text.terms

java -Xmx7512M -server it.unimi.dsi.sux4j.util.SignedFunctionStringMap $WORK_DIR/gov2-text.mwhc $WORK_DIR/gov2-text.termmap

endtime=$(date +%s)

echo "Indexing time: $((endtime-starttime))s"


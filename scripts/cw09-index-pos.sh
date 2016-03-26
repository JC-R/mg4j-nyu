#!/bin/bash
set -e

#sudo apt-add-repository -y ppa:webupd8team/java
#sudo apt-get -y update
#sudo apt-get -y install oracle-java8-installer
#sudo apt-get -y install ruby

version=5.4.1

source ../common.sh

WORK_DIR=/home/juan/work/IR/ClueWeb09b/index-pos
MEDIA=/home/juan/media/ClueWeb09b/ClueWeb09_English_1

#if [[ ! -f mg4j-big-$version-bin.tar.gz ||  ! -f mg4j-big-deps.tar.gz ]]; then
#	curl http://mg4j.di.unimi.it/mg4j-big-$version-bin.tar.gz >mg4j-big-$version-bin.tar.gz
#	curl http://mg4j.di.unimi.it/mg4j-big-deps.tar.gz >mg4j-big-deps.tar.gz
#fi
#
#tar -zxvf mg4j-big-$version-bin.tar.gz
#tar -zxvf mg4j-big-deps.tar.gz

export CLASSPATH=.:$(find -iname \*.jar | paste -d: -s)

starttime=$(date +%s)

# Parallel

rm -f $WORK_DIR/cw09.titles $WORK_DIR/cw09-text.* $WORK_DIR/cw09-split-* split-*

TMP=$(mktemp)
find $MEDIA -iname \*.gz -type f | sort >$TMP
split -n l/8 $TMP split-

(for split in split-*; do
(

	java -Xmx6428M -server \
		it.unimi.di.big.mg4j.document.WarcDocumentSequence_NYU \
			-z -f it.unimi.di.big.mg4j.document.HtmlDocumentFactory -p encoding=iso-8859-1 $WORK_DIR/cw09-$split.sequence $(cat $split)

	java -Xmx6428M -server -Dit.unimi.di.law.warc.io.version=false -Dit.unimi.di.law.warc.records.useburl=false \
		it.unimi.di.big.mg4j.tool.Scan -s 1000000 -S $WORK_DIR/cw09-$split.sequence -t EnglishStemmer -I text $WORK_DIR/cw09-$split >$WORK_DIR/$split.out 2>$WORK_DIR/$split.err

)& 

done

wait)

# Check that all instances have completed

if (( $(find $WORK_DIR -iname cw09-split-\*-text.cluster.properties | wc -l) != 8 )); then
	echo "ERROR: Some instance did not complete correctly" 1>&2
	exit 1
fi

java -Xmx7512M -server it.unimi.di.big.mg4j.tool.Concatenate $WORK_DIR/cw09-text \
	$(find $WORK_DIR -iname cw09-split-\*-text@\*.sizes | sort | sed s/.sizes//)
cat $(find $WORK_DIR -iname cw09-split-\*.titles | sort) >$WORK_DIR/cw09.titles

java -Xmx7512M -server it.unimi.dsi.sux4j.mph.MWHCFunction -s 32 $WORK_DIR/cw09-text.mwhc $WORK_DIR/cw09-text.terms

java -Xmx7512M -server it.unimi.dsi.sux4j.util.SignedFunctionStringMap $WORK_DIR/cw09-text.mwhc $WORK_DIR/cw09-text.termmap


endtime=$(date +%s)

echo "Indexing time: $((endtime-starttime))s"



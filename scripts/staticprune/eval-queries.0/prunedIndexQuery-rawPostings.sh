#!/bin/bash
set -e

#
# run eval queries over a pruned index; collect raws postings
if [ "$#" -lt 8 ]; then
    echo \n"parameters: CORPUS RESULTS HOME_DIR WORK_DIR INDEX_DIR BASELINE PRUNED_DIR TREC_TOPICS\n"
    exit 1
fi

# parameters
#CORPUS=gov2,cw09b
#RESULTS=results
#HOME_DIR=/home/juan/work/data
#WORK_DIR=$HOME_DIR/experiments
#INDEX_DIR=$HOME_DIR/IR/$CORPUS/index/mg4j/
#BASELINE_INDEX=$INDEX_DIR/qs-xdoc/$CORPUS-text
#PRUNED_DIR=$INDEX_DIR/pruned
#TOPICS=true
#MODEL=null

MY_PATH="`dirname \"$0\"`"              # relative
MY_PATH="`( cd \"$MY_PATH\" && pwd )`"  # absolutized and normalized
if [ -z "$MY_PATH" ] ; then
  # error; for some reason, the path is not accessible
  # to the script (e.g. permissions re-evaled after suid)
  exit 1  # fail
fi

export PATH=.:$PATH
export CLASSPATH=~/code/mg4j-nyu/target/*

CORPUS=$1
RESULTS=$2
HOME_DIR=$3
WORK_DIR=$4
INDEX_DIR=$5
BASELINE_INDEX=$6
PRUNED_DIR=$7
if [ "$8" -eq "1" ]; then TOPICS=--trecQueries; fi
MODEL=$9

# clear previous results
PWD=`pwd`
cd $WORK_DIR
NUM_RESULTS=1000

#rm -f $RESULTS/*.postings.txt
#rm -f $RESULTS/*.postings.log
#rm ~/code/mg4j-nyu/target/log4j-over*

# create query set
#run_params="\$score BM25PrunedScorer(1.2,0.3)\n\$limit $NUM_RESULTS\n\$mplex off"
#cat <(echo -e $run_params) <(cat lists/$CORPUS.topics.AND.txt) >lists/$CORPUS.trec_eval.AND.txt
#cat <(echo -e $run_params) <(cat lists/$CORPUS.topics.OR.txt) >lists/$CORPUS.trec_eval.OR.txt
#cat <(cat topics-and-qrels/qrels.701-750.txt) <(cat topics-and-qrels/qrels.751-800.txt) <(cat topics-and-qrels/qrels.801-850.txt) > $RESULTS/qrels.txt

OPTIONS="-server -Dlogback.configurationFile=logback.xml"

# NOTE: need to use the same version of SUX4J on all of these; currently the index was built on V3.
# make sure ALL indexes are built on the same version

## baseline
#ARGS="-r $NUM_RESULTS $TOPICS --trec -I lists/$CORPUS.trec_eval.$s.txt -T $INDEX_DIR/qs-xdoc/$CORPUS.titles $BASELINE_INDEX"
#java $OPTIONS edu.nyu.tandon.experiments.StaticPruning.RawPostingsPrunedQuery -d $RESULTS/$CORPUS-baseline-$s-.postings.txt $ARGS 2>$RESULTS/$CORPUS-baseline-$s.postings.log &
#java $OPTIONS edu.nyu.tandon.experiments.StaticPruning.RawPostingsPrunedQuery -d $RESULTS/$CORPUS-baseline-$s-.txt --trecOutput $ARGS 2>$RESULTS/$CORPUS-baseline-$s.log &
#done
#wait

#mv ~/code/mg4j-nyu/target/sux4j-3* ./

# prune levels
#for n in 01 02 03 04 05 10 15 20 25 30; do
for n in 01 02 03 04 05 10 15 20; do
for k in top1k top10; do
for s in OR AND; do

#  **requires** a pruned index with global stats
# -----------------------------------------------
INDEX=$PRUNED_DIR/$CORPUS$MODEL.$k-$n
fname=$CORPUS$MODEL.$k-$n
ARGS="-r $NUM_RESULTS $TOPICS --trec --globalScoring -I lists/$CORPUS.trec_eval.$s.txt -T $PRUNED_DIR/$fname.titles"

# get results - use global index stats
#java -Xmx60g $OPTIONS edu.nyu.tandon.experiments.RawPostingsPrunedQuery -d $RESULTS/$fname-$s-local.postings.txt $ARGS 2>>$RESULTS/$fname-$s-local.postings.log
java -Xmx15g $OPTIONS edu.nyu.tandon.experiments.StaticPruning.RawPostingsPrunedQuery $ARGS -d $RESULTS/$fname-$s.postings.txt $INDEX-0 2>$RESULTS/$fname-$s.postings.log &
java -Xmx15g $OPTIONS edu.nyu.tandon.experiments.StaticPruning.RawPostingsPrunedQuery $ARGS -d $RESULTS/$fname-$s.txt --trecOutput $INDEX-0 2>$RESULTS/$fname-$s.log &

# TODO: eval results
# ------------

done
wait
done
done

#mv ./sux4j-3* ~/code/mg4j-nyu/target/

cd $PWD

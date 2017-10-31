#!/bin/bash
set -e

# run eval queries over an index; collect raws postings & trec evals

# parameters
#RESULTS=results
#HOME_DIR=/home/juan/work/data
#WORK_DIR=$HOME_DIR/experiments
#INDEX_DIR=$HOME_DIR/IR/$CORPUS/index/mg4j/
#BASELINE_INDEX=$INDEX_DIR/qs-xdoc/$CORPUS-text
#PRUNED_DIR=$INDEX_DIR/pruned
#TOPICS=1
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

INDEX_DIR=$1
CORPUS=$2
WORK_DIR=$3
RESULTS=$4

PWD=`pwd`
cd $WORK_DIR

# clear previous results
#rm -f $RESULTS/*.postings.txt
#rm -f $RESULTS/*.postings.log

NUM_RESULTS=1000

# create query set
#run_params="\$score BM25PrunedScorer(1.2,0.3)\n\$limit $NUM_RESULTS\n\$mplex off"
#cat <(echo -e $run_params) <(cat lists/$CORPUS.topics.AND.txt) >lists/$CORPUS.trec_eval.AND.txt
#cat <(echo -e $run_params) <(cat lists/$CORPUS.topics.OR.txt) >lists/$CORPUS.trec_eval.OR.txt
#cat <(cat topics-and-qrels/qrels.701-750.txt) <(cat topics-and-qrels/qrels.751-800.txt) <(cat topics-and-qrels/qrels.801-850.txt) > $RESULTS/qrels.txt

# NOTE: need to use the same version of SUX4J on all of these; currently the index was built on V3.
# make sure ALL indexes are built on the same version

TREC="--trecOutput --trec"
export _JAVA_OPTIONS=

ARGS="-r $NUM_RESULTS -T $INDEX_DIR/$CORPUS.titles $INDEX_DIR/$CORPUS-text"
CMD="-server -Dlogback.configurationFile=logback.xml edu.nyu.tandon.experiments.staticpruning.RawPostingsPrunedQuery"


for s in OR AND; do
java -Xmx10g $CMD $TREC -I $WORK_DIR/lists/$CORPUS.trec_eval.$s.txt -d $RESULTS/$CORPUS.baseline.$s.txt $ARGS 2>$RESULTS/$CORPUS.baseline.$s.log &
java -Xmx10g $CMD -I $WORK_DIR/lists/held.66k.$s -d $RESULTS/$CORPUS.baseline.$s.postings.txt $ARGS 2>$RESULTS/$CORPUS.baseline.$s.postings.log &
done
wait

# for n in 25 30; do
# FNAME=$PRUNED_CORPUS.$k-$n
# ARGS="-r $NUM_RESULTS --globalScoring -T $INDEX_DIR/$FNAME.titles $INDEX_DIR/$FNAME-0"
# java -Xmx20g $CMD -I $WORK_DIR/lists/held.66k.$s -d $RESULTS/$FNAME.$s.postings.txt $ARGS 2>$RESULTS/$FNAME.$s.postings.log &
# done
# wait

# TODO: eval results
# ------------


cd $PWD

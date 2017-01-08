#!/bin/bash
set -e

MY_PATH="`dirname \"$0\"`"              # relative
MY_PATH="`( cd \"$MY_PATH\" && pwd )`"  # absolutized and normalized
if [ -z "$MY_PATH" ] ; then
  # error; for some reason, the path is not accessible
  # to the script (e.g. permissions re-evaled after suid)
  exit 1  # fail
fi

# Runs the MG4J k-out-of-n queries and performs evaluation
export CLASSPATH=$MY_PATH/../../*
export PATH=.:$PATH

# parameters
#CORPUS=gov2
#RESULTS=results
#HOME_DIR=/home/juan/work/data
#WORK_DIR=$HOME_DIR/experiments
#INDEX_DIR=$HOME_DIR/IR/$CORPUS/index/mg4j/
#BASELINE_INDEX=$INDEX_DIR/qs-xdoc/$CORPUS-text
#PRUNED_DIR=$INDEX_DIR/pruned

CORPUS=$1
RESULTS=$2
HOME_DIR=$3
WORK_DIR=$4
INDEX_DIR=$5
BASELINE_INDEX=$6
PRUNED_DIR=$7

# clear previous results
PWD=`pwd`
cd $WORK_DIR
NUM_RESULTS=1000

rm -f $RESULTS/*.postings.txt
rm -f $RESULTS/*.postings.log

run_params="\$score BM25PrunedScorer(1.2,0.3)\n\$limit $NUM_RESULTS\n\$mplex off"
cat <(echo -e $run_params) <(cat lists/$CORPUS.topics.AND.txt) >lists/$CORPUS.trec_eval.AND.txt
cat <(echo -e $run_params) <(cat lists/$CORPUS.topics.OR.txt) >lists/$CORPUS.trec_eval.OR.txt
cat <(cat topics-and-qrels/qrels.701-750.txt) <(cat topics-and-qrels/qrels.751-800.txt) <(cat topics-and-qrels/qrels.801-850.txt) > $RESULTS/qrels.txt

OPTIONS="-server -Dlogback.configurationFile=logback.xml"

# baseline trec queries
for s in AND OR; do
ARGS="-r $NUM_RESULTS --trecQueries --trec -I lists/$CORPUS.trec_eval.$s.txt -T $INDEX_DIR/qs-xdoc/$CORPUS.titles $BASELINE_INDEX"
java $OPTIONS edu.nyu.tandon.experiments.RawPostingsPrunedQuery -d $RESULTS/$CORPUS-baseline-$s-.postings.txt $ARGS 2>>$RESULTS/$CORPUS-baseline-$s.postings.log &
done

# prune levels
#for n in 01 02 03 04 05 10 15 20 25 30; do
for n in 01 02 03 04 05 10 15 20; do
for k in top10 top1k; do
for s in AND OR; do

#  requires pruned index
# ----------------------
INDEX=$PRUNED_DIR/$CORPUS-$k-$n
fname=$CORPUS-$k-$n
ARGS="-r $NUM_RESULTS  --trecQueries --trec -I lists/$CORPUS.trec_eval.$s.txt -T $PRUNED_DIR/$fname.titles $INDEX-0"

# get results
java -Xmx60g $OPTIONS edu.nyu.tandon.experiments.RawPostingsPrunedQuery -d $RESULTS/$fname-$s-local.postings.txt $ARGS 2>>$RESULTS/$fname-$s-local.postings.log
java -Xmx60g $OPTIONS edu.nyu.tandon.experiments.RawPostingsPrunedQuery --globalScoring -d $RESULTS/$fname-$s-global.postings.txt $ARGS 2>>$RESULTS/$fname-$s-global.postings.log &

# TODO: eval results
# ------------

done
wait
done
done

cd $PWD

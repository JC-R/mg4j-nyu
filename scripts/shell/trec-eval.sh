#!/usr/bin/env bash

if [ "$#" -ne 1 ]; then
    echo "Illegal number of parameters: trec-eval.sh <results_dir>"
    exit 1
fi

HOME_DIR=/home/juan/work/data/experiments
NUM_RESULTS=1000

# params: results directory
PWD=`pwd`
cd $HOME_DIR

# prepare the queries
#run_params="\$score BM25PrunedScorer(1.2,0.3)\n\$limit $NUM_RESULTS\n\$mplex off"
#cat <(echo -e $run_params) <(cat lists/topics.AND.txt) >lists/trec_eval.AND.txt
#cat <(echo -e $run_params) <(cat lists/topics.OR.txt) >lists/trec_eval.OR.txt
#cat <(cat topics-and-qrels/qrels.701-750.txt) <(cat topics-and-qrels/qrels.751-800.txt) <(cat topics-and-qrels/qrels.801-850.txt) > $1/qrels.txt

#BASE_DIR=/home/juan/work/data/IR/Gov2/index/mg4j
#CORPUS=gov2
#for m in dh dh_s; do
#~/work/code/mg4j-nyu/scripts/shell/PrunedIndex-trec-eval.sh $CORPUS $m $BASE_DIR qs-xdoc/$CORPUS-text pruned $1 $HOME_DIR
#done

BASE_DIR=/home/juan/work/data/IR/ClueWeb09b/index/mg4j
CORPUS=cw09b
for m in dh dh_s; do
~/work/code/mg4j-nyu/scripts/shell/PrunedIndex-trec-eval.sh $CORPUS $m $BASE_DIR qs-xdoc/$CORPUS-text pruned $1 $HOME_DIR
done

cd $PWD

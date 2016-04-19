#!/bin/bash
set -e

# Runs the MG4J k-out-of-n queries and performs evaluation
export CLASSPATH=/home/juan/sandbox/mg4j-nyu/*:/home/juan/work/runtime/*
export PATH=.:$PATH

CORPUS=gov2
NUM_RESULTS=1000

OPTIONS="-server -Dlogback.configurationFile=logback.xml"

INDEX_DIR=/home/juan/work/IR/Gov2/index/mg4j/
BASELINE_INDEX=$INDEX_DIR/qs-xdoc/$CORPUS-text
PRUNED_DIR=$INDEX_DIR/pruned

WORK_DIR=/home/juan/work/experiments
PWD=`pwd`
cd $WORK_DIR

run_params="\$score BM25PrunedScorer(1.2,0.3)\n\$limit $NUM_RESULTS\n\$mplex off"
cat <(echo -e $run_params) <(cat lists/$CORPUS.topics.AND.txt) >lists/$CORPUS.trec_eval.AND.txt
cat <(echo -e $run_params) <(cat lists/$CORPUS.topics.OR.txt) >lists/$CORPUS.trec_eval.OR.txt
cat <(cat topics-and-qrels/qrels.701-750.txt) <(cat topics-and-qrels/qrels.751-800.txt) <(cat topics-and-qrels/qrels.801-850.txt) > results/qrels.txt

#

# prune levels
#for n in {1,2,3,4,5,6,7,8,9,10,15,20,25,30,35,40,45,50,75,100}

for n in 01 02 03 04 05 10 15 20; do
for k in top10 top1k; do
for s in AND OR; do

#  requires pruned index
# ----------------------
INDEX=$PRUNED_DIR/$CORPUS-$k-$n
fname=$CORPUS-$k-$n

java $OPTIONS edu.nyu.tandon.experiments.PrunedQuery --trec --globalScoring -I lists/$CORPUS.trec_eval.$s.txt -T $PRUNED_DIR/$fname.titles -d results/$fname-$s-global.txt $INDEX-0 2>>results/$fname-$s-global.log &
java $OPTIONS edu.nyu.tandon.experiments.PrunedQuery --trec -I lists/$CORPUS.trec_eval.$s.txt -T $PRUNED_DIR/$fname.titles -d results/$fname-$s-local.txt $INDEX-0 2>>results/$fname-$s-local.log &

wait

# eval results
# ------------
for scoring in local global; do
trec_eval -q results/qrels.txt results/$fname-$s-$scoring.txt >results/eval.$fname-$s-$scoring
grep ms\; results/$fname-$s-$scoring.log | cut -d' ' -f6 | paste -d+ -s | bc -l >results/time.$fname-$s-$scoring
done
done
done
done

#collect  results
echo "dataset,target_label,prune_size,query_semantics,scoring,metric,value" >  eval.$CORPUS-ph.csv
grep '[[:space:]]all[[:space:]]' results/eval.$CORPUS-top1?-??-* | tr ':/.\t-' ' ' | sed -e 's/_0 /_0./g' -e 's/[[:space:]]0 / 0./g' -e 's/results eval //g' -e 's/none //' -e 's/all //g' -e 's/_1 /_1./g' -e 's/  \+/ /g' | tr ' ' ',' | sed -e 's/^/ph,/g' > eval.$CORPUS-ph.csv

cd $PWD

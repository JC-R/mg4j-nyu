#!/bin/bash
set -e

# Runs the MG4J k-out-of-n queries and performs evaluation
export CLASSPATH=/home/juan/work/sandbox/mg4j-nyu/*
export PATH=.:$PATH

CORPUS=gov2

OPTIONS="-server -Dlogback.configurationFile=logback.xml"

INDEX_DIR=/home/juan/work/data/IR/Gov2/index/mg4j/
BASELINE_INDEX=$INDEX_DIR/qs-xdoc/$CORPUS-text
PRUNED_DIR=$INDEX_DIR/pruned

WORK_DIR=/home/juan/work/data/experiments
PWD=`pwd`
cd $WORK_DIR
NUM_RESULTS=1000

rm -f results/*

run_params="\$score BM25PrunedScorer(1.2,0.3)\n\$limit $NUM_RESULTS\n\$mplex off"
cat <(echo -e $run_params) <(cat lists/$CORPUS.topics.AND.txt) >lists/$CORPUS.trec_eval.AND.txt
cat <(echo -e $run_params) <(cat lists/$CORPUS.topics.OR.txt) >lists/$CORPUS.trec_eval.OR.txt
cat <(cat topics-and-qrels/qrels.701-750.txt) <(cat topics-and-qrels/qrels.751-800.txt) <(cat topics-and-qrels/qrels.801-850.txt) > results/qrels.txt

# baseline trec queries

# get TREC ids for relevance
for s in AND OR; do
ARGS="-r $NUM_RESULTS --trecQueries -I lists/$CORPUS.trec_eval.$s.txt -T $INDEX_DIR/qs-xdoc/$CORPUS.titles $BASELINE_INDEX"
java $OPTIONS edu.nyu.tandon.experiments.PrunedQuery --trec -d results/$CORPUS-baseline-$s-global.txt $ARGS 2>>results/$CORPUS-baseline-$s-global.log &
done

# overlap baselines
java $OPTIONS edu.nyu.tandon.experiments.PrunedQuery -r $NUM_RESULTS --trecQueries -I lists/$CORPUS.trec_eval.AND.txt -d results/$CORPUS.baseline.AND.1k.txt $BASELINE_INDEX 2> results/$CORPUS.trec_eval.baseline.1k-AND.log &
java $OPTIONS edu.nyu.tandon.experiments.PrunedQuery -r $NUM_RESULTS --trecQueries -I lists/$CORPUS.trec_eval.OR.txt -d results/$CORPUS.baseline.OR.1k.txt $BASELINE_INDEX 2> results/$CORPUS.trec_eval.baseline.1k-OR.log  &
wait

for q in AND OR; do
trec_eval -q results/qrels.txt results/$CORPUS-baseline-$q-global.txt > results/eval.$CORPUS-baseline-100-$q-global
grep ms\; results/$CORPUS.trec_eval.baseline.1k-$q.log | cut -d' ' -f6 | paste -d+ -s | bc -l >results/time.$CORPUS.baseline.$q
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
ARGS="-r $NUM_RESULTS --trecQueries -I lists/$CORPUS.trec_eval.$s.txt -T $PRUNED_DIR/$fname.titles $INDEX-0"

# get documents for overlap
java -Xmx60g $OPTIONS edu.nyu.tandon.experiments.PrunedQuery -d results/$fname-$s-local.1k -s $PRUNED_DIR/prune-$CORPUS-$k-$n.strategy $ARGS 2>>results/$fname-$s-local.1k.log
java -Xmx60g $OPTIONS edu.nyu.tandon.experiments.PrunedQuery --globalScoring -d results/$fname-$s-global.1k -s $PRUNED_DIR/prune-$CORPUS-$k-$n.strategy $ARGS 2>>results/$fname-$s-global.1k.log &

# get TREC ids for relevance
java $OPTIONS edu.nyu.tandon.experiments.PrunedQuery --trec -d results/$fname-$s-local.txt $ARGS 2>>results/$fname-$s-local.log &
java $OPTIONS edu.nyu.tandon.experiments.PrunedQuery --trec --globalScoring -d results/$fname-$s-global.txt $ARGS 2>>results/$fname-$s-global.log &

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
grep '[[:space:]]all[[:space:]]' results/eval.$CORPUS-* | tr ':/.\t-' ' ' | sed -e 's/_0 /_0./g' -e 's/[[:space:]]0 / 0./g' -e 's/results eval //g' -e 's/none //' -e 's/all //g' -e 's/_1 /_1./g' -e 's/  \+/ /g' | tr ' ' ',' | sed -e 's/^/ph,/g' >> eval.$CORPUS-ph.csv

$PWD/extract-overlap.sh

cd $PWD

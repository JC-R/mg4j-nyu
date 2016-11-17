#!/bin/bash
set -e

# parameters
#  1: corpus (gov2, cw09b)

#  2: baseline index
#  3: pruned index directory
#  4: output directory $HOME_DIR

if [ "$#" -ne 6 ]; then
    echo "Illegal number of parameters: PrnedIndex-trec-eval.sh CORPUS MODEL_NAME BASELINE_INDEX PRUNED_INDER_DIR OUTPUT_DIR RUN_DIRECTORY"
    exit 1
fi

CORPUS=$1
MODEL=$2
BASELINE_INDEX=$3
PRUNED_DIR=$4
RESULTS=$5
WORK_DIR=$6

# get scritp path
CWD=`pwd`
MY_PATH="`dirname \"$0\"`"              # relative
MY_PATH="`( cd \"$MY_PATH\" && pwd )`"  # absolutized and normalized
if [ -z "$MY_PATH" ] ; then
  # error; for some reason, the path is not accessible
  # to the script (e.g. permissions re-evaled after suid)
  exit 1  # fail
fi

cd $WORK_DIR

# Runs the MG4J k-out-of-n queries and performs evaluation
export CLASSPATH=/home/juan/work/code/mg4j-nyu/*
export PATH=.:$PATH

NUM_RESULTS=1000

rm -f $RESULTS/*

OPTIONS="-server -Dlogback.configurationFile=logback.xml"

run_params="\$score BM25PrunedScorer(1.2,0.3)\n\$limit $NUM_RESULTS\n\$mplex off"
cat <(echo -e $run_params) <(cat lists/$CORPUS.topics.AND.txt) >lists/$CORPUS.trec_eval.AND.txt
cat <(echo -e $run_params) <(cat lists/$CORPUS.topics.OR.txt) >lists/$CORPUS.trec_eval.OR.txt
cat <(cat topics-and-qrels/qrels.701-750.txt) <(cat topics-and-qrels/qrels.751-800.txt) <(cat topics-and-qrels/qrels.801-850.txt) > $RESULTS/qrels.txt

# baseline
# ---------
# ---------

# get TREC ids for relevance
for s in AND OR; do
ARGS="-r $NUM_RESULTS --trecQueries -I lists/$CORPUS.trec_eval.$s.txt -T $INDEX_DIR/qs-xdoc/$CORPUS.titles $BASELINE_INDEX"
java $OPTIONS edu.nyu.tandon.experiments.PrunedQuery --trec -d $RESULTS/$CORPUS-baseline-$s-global.txt $ARGS 2>>$RESULTS/$CORPUS-baseline-$s-global.log &
done

# overlap baselines
# -----------------
java $OPTIONS edu.nyu.tandon.experiments.PrunedQuery -r $NUM_RESULTS --trecQueries -I lists/$CORPUS.trec_eval.AND.txt -d results/$CORPUS.baseline.AND.1k.txt $BASELINE_INDEX 2> $RESULTS/$CORPUS.trec_eval.baseline.1k-AND.log &
java $OPTIONS edu.nyu.tandon.experiments.PrunedQuery -r $NUM_RESULTS --trecQueries -I lists/$CORPUS.trec_eval.OR.txt -d results/$CORPUS.baseline.OR.1k.txt $BASELINE_INDEX 2> $RESULTS/$CORPUS.trec_eval.baseline.1k-OR.log  &
wait

for q in AND OR; do
trec_eval -q $RESULTS/qrels.txt $RESULTS/$CORPUS-baseline-$q-global.txt > $RESULTS/eval.$CORPUS-baseline-100-$q-global
grep ms\; $RESULTS/$CORPUS.trec_eval.baseline.1k-$q.log | cut -d' ' -f6 | paste -d+ -s | bc -l >$RESULTS/time.$CORPUS.baseline.$q
done

# pruned evals
# --------------
# --------------

# prune levels
for n in 01 02 03 04 05 10 15 20 25 30; do
for s in AND OR; do

#  requires pruned index
# ----------------------
INDEX=$PRUNED_DIR/$CORPUS-$MODEL-$n
fname=$CORPUS-$MODEL-$n
ARGS="-r $NUM_RESULTS --trecQueries -I lists/$CORPUS.trec_eval.$s.txt -T $PRUNED_DIR/$fname.titles $INDEX-0"

# get documents for overlap
java -Xmx60g $OPTIONS edu.nyu.tandon.experiments.PrunedQuery -d $RESULTS/$fname-$s-local.1k -s $PRUNED_DIR/prune-$CORPUS-$MODEL-$n.strategy $ARGS 2>>$RESULTS/$fname-$s-local.1k.log
java -Xmx60g $OPTIONS edu.nyu.tandon.experiments.PrunedQuery --globalScoring -d $RESULTS/$fname-$s-global.1k -s $PRUNED_DIR/prune-$CORPUS-$MODEL-$n.strategy $ARGS 2>>$RESULTS/$fname-$s-global.1k.log &

# get TREC ids for relevance
java $OPTIONS edu.nyu.tandon.experiments.PrunedQuery --trec -d $RESULTS/$fname-$s-local.txt $ARGS 2>>$RESULTS/$fname-$s-local.log &
java $OPTIONS edu.nyu.tandon.experiments.PrunedQuery --trec --globalScoring -d $RESULTS/$fname-$s-global.txt $ARGS 2>>$RESULTS/$fname-$s-global.log &

wait

# eval results
# ------------

for scoring in local global; do
trec_eval -q $RESULTS/qrels.txt $RESULTS/$fname-$s-$scoring.txt >$RESULTS/eval.$fname-$s-$scoring
grep ms\; $RESULTS/$fname-$s-$scoring.log | cut -d' ' -f6 | paste -d+ -s | bc -l >$RESULTS/time.$fname-$s-$scoring
done

done
done

#collect  results
echo "dataset,target_label,prune_size,query_semantics,scoring,metric,value" >  $RESULTS/eval.$CORPUS-$MODEL.csv
grep '[[:space:]]all[[:space:]]' $RESULTS/eval.$CORPUS-* | tr ':/.\t-' ' ' | sed -e 's/_0 /_0./g' -e 's/[[:space:]]0 / 0./g' -e 's/results eval //g' -e 's/none //' -e 's/all //g' -e 's/_1 /_1./g' -e 's/  \+/ /g' | tr ' ' ',' | sed -e 's/^/ph,/g' >> $RESULTS/eval.$CORPUS-$MODEL.csv

$CLASSPATH/scripts/extract-overlap $RESULTS

cd $PWD

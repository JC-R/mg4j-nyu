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

run_params="\$score BM25Scorer(1.2,0.3)\n\$limit $NUM_RESULTS\n\$mplex off"

#cat <(echo -e $run_params) <(cat lists/$CORPUS.topics.AND.txt) >lists/$CORPUS.trec_eval.AND.txt
#cat <(echo -e $run_params) <(cat lists/$CORPUS.topics.OR.txt) >lists/$CORPUS.trec_eval.OR.txt
#cat <(cat topics-and-qrels/qrels.701-750.txt) <(cat topics-and-qrels/qrels.751-800.txt) <(cat topics-and-qrels/qrels.801-850.txt) > results/qrels.txt

#

# prune levels
#for n in {1,2,3,4,5,6,7,8,9,10,15,20,25,30,35,40,45,50,75,100}

for n in 01 02 03 04 05 10 15; do
for k in top10 top1k; do
for s in AND OR; do
#for f in lists/*.ml; do

#fname=$(echo $f | cut -d '/' -f 2)
#RUN=results/run.$fname.trec_eval.prune-$n.$s.txt
#ERR=results/err.$fname.trec_eval.prune-$n.$s.txt

# doc pruning - on the fly (simulated pruning)
#   requires: --prune -I <doc-list> -T <titles> -s <threshold>
# --------------------------------------------
#java -server -Dlogback.configurationFile=logback.xml edu.nyu.tandon.experiments.PrunedQuery --trec -T $INDEX_DIR/$CORPUS.titles --prune -s $n -L $f $BASELINE_INDEX -I lists/$CORPUS.trec_eval.$s.txt -d $run 2>>$err

# posthits pruning - actual index
#   requires full pruned index
# -------------------------------
INDEX=$PRUNED_DIR/$CORPUS-$k-$n-0

java $OPTIONS edu.nyu.tandon.experiments.PrunedQuery --trec -I lists/$CORPUS.trec_eval.$s.txt -d results/$CORPUS.pruned-$n.txt $INDEX 2>>$ERR

# eval results
# ------------
trec_eval -q results/qrels.txt $run >results/eval.$fname.trec_eval.$n.$s
grep ms\; $err | cut -d' ' -f6 | paste -d+ -s | bc -l >results/time.$fname.trec_eval.$n.$s

#done
done
done
done

#collect  results
echo "dataset,training_set,model,target_label,query_set,prune_size,query_semantics,metric,value" >  eval.$CORPUS.csv
grep '[[:space:]]all[[:space:]]' results/eval.$CORPUS.* | tr ':/.\t' ' ' | sed -e 's/_0 /_0./g' -e 's/[[:space:]]0 / 0./g' -e 's/results eval //g' -e 's/none //' -e 's/all //g' -e 's/ ml / /g' -e 's/[[:space:]][[:space:]]*/ /g' -e 's/_1 /_1./g' | tr ' ' ',' >> eval.$CORPUS.csv



#!/bin/bash
set -e

if [ "$#" -ne 6 ]; then
    echo "Illegal number of parameters"
    exit 1
fi

# Runs the MG4J k-out-of-n queries and performs evaluation
export CLASSPATH=$1/*:$CLASSPATH
export PATH=.:$PATH

OPTIONS="-Xmx12g -server -Dlogback.configurationFile=logback.xml"

CORPUS=$2
RESULTS=$3
BASELINE_DIR=$4
PRUNED_DIR=$5
WORK_DIR=$6
BASELINE=$BASELINE_DIR/$CORPUS-text

PWD=`pwd`
cd $WORK_DIR

rm -f $RESULTS/*.overlap.*
rm -f $RESULTS/*-overlap-*

cat <(cat lists/$CORPUS.topics.AND.txt) >lists/$CORPUS.trec_eval.AND.txt
cat <(cat lists/$CORPUS.topics.OR.txt) >lists/$CORPUS.trec_eval.OR.txt

# prune levels
for n in 01 02 03 04 05 10 15 20; do
for k in top10 top1k; do
for s in AND OR; do

INDEX=$PRUNED_DIR/$CORPUS-$k-$n
fname=$CORPUS-$k-$n-$s

# for index independent features, use flags: --trec
# for actual postings, use -d $RESULTS/$fname-overlap-10.raw

# top 10
java $OPTIONS edu.nyu.tandon.experiments.PrunedIndexOverlap -r 10 -T $BASELINE_DIR/$CORPUS.titles -t $INDEX.titles -I lists/$CORPUS.trec_eval.$s.txt -i $INDEX-0 $BASELINE >$RESULTS/$fname-overlap-10.txt  &

# top 1K
java $OPTIONS edu.nyu.tandon.experiments.PrunedIndexOverlap -r 1000 -T $BASELINE_DIR/$CORPUS.titles -t $INDEX.titles -I lists/$CORPUS.trec_eval.$s.txt -i $INDEX-0 $BASELINE >$RESULTS/$fname-overlap-1k.txt &

done
done
wait
done

# assemble results file

#echo "corpus,prune_size,target_label,semantics,results_size,rk,pk" > $RESULTS/$CORPUS.overlap.csv

for n in 01 02 03 04 05 10 15 20; do
for k in top10 top1k; do
for s in AND OR; do
fname=$CORPUS-$k-$n-$s
for r in 10 1k; do
echo "$CORPUS,$n,$k,$s,$r" | paste -d ',' - $RESULTS/$fname-overlap-$r.txt >> $RESULTS/$CORPUS.overlap.csv
done
done
done
done

cd $PWD


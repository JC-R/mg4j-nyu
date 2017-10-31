#!/bin/bash
set -e

cd /mnt/ssd

PREFIX=cw09b

# baseline
# --------
for s in OR AND; do

fname=$PREFIX.baseline.$s
sed '/null/d' results/$fname.txt > results/$fname-temp.txt
/home/juan/code/trec_eval/trec_eval lists/cw09b.qrels.txt results/$fname-temp.txt > results/$fname.eval.txt
rm results/$fname-temp.txt
grep ms\; results/$PREFIX-baseline.$s.log | cut -d' ' -f6 | paste -d+ -s | bc -l >results/$fname.time.txt

done


# eval results
# ------------

for n in 01 02 03 04 05 10 15 20 30; do
for k in top1k; do
for s in OR AND; do

fname=$PREFIX-$k-$n.$s
sed '/null/d' results/$fname.txt > results/$fname-temp.txt
/home/juan/code/trec_eval/trec_eval lists/cw09b.qrels.txt results/$fname-temp.txt > results/$fname.eval.txt
rm results/$fname-temp.txt
grep ms\; results/$fname.log | cut -d' ' -f6 | paste -d+ -s | bc -l >results/$fname.time.txt

done
done
done

#collect  results
echo "learn_set,target_label,model,prune_size,query_semantics,metric,value" >  results/cw09b.csv
grep '[[:space:]]all[[:space:]]' results/$PREFIX*.eval.txt | tr ':/.\t-' ' ' | sed -e 's/_0 /_0./g' -e 's/[[:space:]]0 / 0./g' -e 's/results//g' -e 's/txt//' -e 's/eval//g' -e 's/_1 /_1./g' -e 's/  \+/ /g' -e 's/^ //g' | tr ' ' ',' >> results/cw09b.csv


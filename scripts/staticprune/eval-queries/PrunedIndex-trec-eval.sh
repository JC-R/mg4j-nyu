#!/bin/bash
set -e

WORK_DIR=/mnt/ssd
DATA_DIR=/mnt/work2/results
OUT_DIR=/mnt/work2/results


PREFIX=cw09b

# baseline
# --------
# for s in OR AND; do

# fname=$PREFIX.baseline.$s
# sed '/null/d' $DATA_DIR/$fname.txt > $OUT_DIR/$fname-temp.txt
# /home/juan/code/trec_eval/trec_eval $WORK_DIR/lists/cw09b.qrels.txt $OUT_DIR/$fname-temp.txt > $OUT_DIR/$fname.eval.txt
# rm $OUT_DIR/$fname-temp.txt
# grep ms\; $DATA_DIR/$PREFIX.baseline.$s.log | cut -d' ' -f6 | paste -d+ -s | bc -l >$OUT_DIR/$fname.time.txt

# done


# eval results
# ------------

# # for m in all posthits dochits doc term intrinsic; do
# for m in all posthits dochits intrinsic doc term ; do
# for n in 01 02 03 04 05 10 15 20 25 30; do
# for k in top1k; do
# for s in OR AND; do

# fname=$m.$k-$n.$s
# if [ -e $DATA_DIR/$fname.txt ]
# then
#     echo "$DATA_DIR/$fname.txt"
#     sed '/null/d' $DATA_DIR/$fname.txt > $OUT_DIR/$fname-temp.txt
#     /home/juan/code/trec_eval/trec_eval $WORK_DIR/lists/cw09b.qrels.txt $OUT_DIR/$fname-temp.txt > $OUT_DIR/$fname.eval.txt
#     rm $OUT_DIR/$fname-temp.txt
#     grep ms\; $DATA_DIR/$fname.log | cut -d' ' -f6 | paste -d+ -s | bc -l >$OUT_DIR/$fname.time.txt
# else
#     echo "skipping $DATA_DIR/$fname.txt "
# fi

# done
# done
# done
# done

#collect  P@10 results
echo "learn_set,target_label,model,prune_size,query_semantics,metric,value" >  $DATA_DIR/cw09b.csv
grep '[[:space:]]all[[:space:]]' $OUT_DIR/*.eval.txt | tr ':/.\t-' ' ' | sed -e 's/_0 /_0./g' -e 's/[[:space:]]0 / 0./g' -e 's/results//g' -e 's/txt//' -e 's/eval//g' -e 's/_1 /_1./g' -e 's/  \+/ /g' -e 's/^ //g' | tr ' ' ',' >> $OUT_DIR/$PREFIX.csv


# timing results

# echo "model,query,num_results,documents,total_time" > $OUT_DIR/$PREFIX.times.csv
# for m in all posthits dochits doc term intrinsic; do
# for n in 01 02 03 04 05 10 15 20 25 30; do
# for k in top1k; do
# for s in OR AND; do

# grep '\{text\}' $OUT_DIR/$m.$k-$n.$s.postings.log | grep -n '\{text\}>' | sed -e 's/{text}>/ /g' -e '/it\.unimi/d' -e 's/[:;\/]/ /g' -e 's/results\|document[s]*\|examined\|ms\|ns\|s[,]*//g' -e 's/,//g' -e 's/ \{2,\}/ /g' | cut -d ' ' -f 1,2,3,4 | sed -e 's/ /,/g' -e "s/^/$m,/g" | head -n -1 >> $OUT_DIR/$PREFIX.times.csv

# done
# done
# done
# done

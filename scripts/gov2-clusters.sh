#!/bin/bash
set -e

version=5.4.1

WORK_DIR=~/work
GOV2_LOCATION=~/Gov2
CLUSTER_DIR=~/clustering/gov2-50-bytopic-ak13.v1

export CLASSPATH=.:$(find ../ -iname \*.jar | paste -d: -s)

starttime=$(date +%s)

rm -f $WORK_DIR/gov2-*.titles $WORK_DIR/gov2-*-text.* $WORK_DIR/gov2-*-split-* *split-*

TMP=$(mktemp)
find $GOV2_LOCATION -type f -iname \*.gz | sort >$TMP
split -n l/8 $TMP split-

(for split in split-*; do
(
        java -Xmx7512M -server \
                it.unimi.di.big.mg4j.document.TRECDocumentCollection \
                        -z -f HtmlDocumentFactory -p encoding=iso-8859-1 $WORK_DIR/gov2-$split.collection $(cat $split)

        for cluster in $(find $CLUSTER_DIR -type f -printf "%f\n" | sort -n); do
                printf "Creating subindex %s for cluster %s\n" "$split" "$cluster"
                java -Xmx7512M -server \
                        it.unimi.di.big.mg4j.tool.Scan -C $CLUSTER_DIR/$cluster -s 1000000 -S $WORK_DIR/gov2-$split.collection -t EnglishStemmer -I text -c COUNTS $WORK_DIR/gov2-$cluster-$split >$cluster-$split.1.out 2>$cluster-$split.1.
        done

)&

done

wait)

# Check that all instances have completed

if (( $(find $WORK_DIR -iname gov2-\*-text.cluster.properties | wc -l) != 8 * $(ls $CLUSTER_DIR | wc -l) )); then
        echo "ERROR: Some instance did not complete correctly" 1>&2
        exit 1
fi

for cluster in $(find $CLUSTER_DIR -type f -printf "%f\n"); do
        java -Xmx7512M -server it.unimi.di.big.mg4j.tool.Concatenate -c POSITIONS:NONE $WORK_DIR/gov2-$cluster-text \
                $(find $WORK_DIR -iname gov2-$cluster-split-\*-text@\*.sizes | sort | sed s/.sizes//)
        cat $(find $WORK_DIR -iname gov2-$cluster-split-\*.titles | sort) >$WORK_DIR/gov2-$cluster.titles

        java -Xmx7512M -server it.unimi.dsi.sux4j.mph.MWHCFunction -s 32 $WORK_DIR/gov2-$cluster-text.mwhc $WORK_DIR/gov2-$cluster-text.terms

        java -Xmx7512M -server it.unimi.dsi.sux4j.util.SignedFunctionStringMap $WORK_DIR/gov2-$cluster-text.mwhc $WORK_DIR/gov2-$cluster-text.termmap
done

endtime=$(date +%s)

echo "Indexing time: $((endtime-starttime))s"

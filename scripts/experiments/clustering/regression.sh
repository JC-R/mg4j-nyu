#!/bin/bash

#
# Arguments:
# 1) source dir
# 2) model output file
# 3) feature files prefix
# /home/elshize/phd/features/gov2-top10c
# gov2-trec_eval-queries\.txt

source "${MG4J_NYU_SCRIPTS}/commons.sh"

dir=$1
output=$2
prefix=$3

if [ -z "${dir}" ]; then echo "You have to define the directory."; exit 1; fi;
if [ -z "${output}" ]; then echo "You have to define the model output file."; exit 1; fi;
if [ -z "${prefix}" ]; then echo "You have to define the feature files prefix."; exit 1; fi;


set -x

segmentFeatureFiles=$(find $dir -regex ".*${prefix}.*segmented" | tr '\n' ',' | sed 's/,$//')
segmentFeatures=`mktemp --tmpdir "segment-features-XXX"`

#java edu.nyu.tandon.ml.features.FeatureUnion \
#    --features ${segmentFeatureFiles} \
#    --output ${segmentFeatures}

${SPARK_HOME}/bin/spark-submit --master spark://localhost:7077 \
    --class edu.nyu.tandon.ml.features.FeatureUnion \
    ~/IdeaProjects/mg4j-nyu/target/mg4j-nyu-1.0.jar \
    --features ${segmentFeatureFiles} \
    --output ${segmentFeatures}

clusterFeatureFiles=$(find $dir -regex ".*${prefix}.*scores" | tr '\n' ',' | sed 's/,$//')
allFeatures=`mktemp --tmpdir "all-features-XXX"`

#java edu.nyu.tandon.ml.features.FeatureJoin \
#    --features "${segmentFeatures},${clusterFeatureFiles}" \
#    --output ${allFeatures} \
#    --join-cols id,cluster

${SPARK_HOME}/bin/spark-submit --master spark://localhost:7077 \
    --class edu.nyu.tandon.ml.features.FeatureJoin \
    ${MG4J_NYU_CLASSPATH} \
    --features "${segmentFeatures},${clusterFeatureFiles}" \
    --output ${allFeatures} \
    --join-cols id,cluster

${SPARK_HOME}/bin/spark-submit --master spark://localhost:7077 \
    --class edu.nyu.tandon.ml.regression.RFRegression \
    ${MG4J_NYU_CLASSPATH} \
    --input ${allFeatures} \
    --output ${output} \
    --label-col count

#java edu.nyu.tandon.ml.regression.RFRegression \
#    --input ${allFeatures} \
#    --output ${output} \
#    --label-col count

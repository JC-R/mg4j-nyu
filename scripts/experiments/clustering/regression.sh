#!/bin/bash

#
# Arguments:
# 1) source dir
# 2) model output file
# 3) feature files prefix
#

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

${SPARK_HOME}/bin/spark-submit \
    --class edu.nyu.tandon.ml.features.FeatureUnion \
    ${MG4J_NYU_CLASSPATH} \
    --features ${segmentFeatureFiles} \
    --output ${segmentFeatures}

clusterFeatureFiles=$(find $dir -regex ".*${prefix}.*scores" | tr '\n' ',' | sed 's/,$//')
allFeatures=`mktemp --tmpdir "all-features-XXX"`

${SPARK_HOME}/bin/spark-submit \
    --class edu.nyu.tandon.ml.features.FeatureJoin \
    ${MG4J_NYU_CLASSPATH} \
    --features "${segmentFeatures},${clusterFeatureFiles}" \
    --output ${allFeatures} \
    --join-cols id

${SPARK_HOME}/bin/spark-submit \
    --class edu.nyu.tandon.ml.regression.RFRegression \
    ${MG4J_NYU_CLASSPATH} \
    --input ${allFeatures} \
    --output ${output} \
    --label-col count

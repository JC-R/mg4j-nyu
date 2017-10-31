export CLASSPATH=/san_scratch/juanr/target/*
~/spark-2.1.1-bin-hadoop2.6/bin/spark-submit --master yarn --driver-memory 40g --packages ai.h2o:h2o-genmodel:3.10.4.6 --executor-memory 16g --executor-cores 4 --num-executors 50 --class edu.nyu.tandon.experiments.staticpruning.ml.h2o.MojoPredict /san_scratch/juanr/target/mg4j-nyu.jar $1 cw09b.raw_features.parquet $2


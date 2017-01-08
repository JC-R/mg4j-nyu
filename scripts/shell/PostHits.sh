#!/usr/bin/env bash

# collect posthits given input query set
# params:
#  1 - input queries
#  2 - output file
#  3 - results to score
#  4 - dump batch size
#  5 - extra flag
#  last param - index

export CLASSPATH=/home/juan/sandbox/mg4j-nyu/*:/home/juan/work/runtime/*
export _JAVA_OPTIONS="-Xmx32g"
NUM_RESULTS=1280

# generate posting raw hits
java -Dlogback.configurationFile=logback.xml edu.nyu.tandon.experiments.RawHits -r $3 -I $1 -d $2 --dumpsize $4 $5 $6

# combine/aggregate into bins: 
#   first param is input directory
#   second parameter is file pattern in regex format
#   third param is # results
#   fourth param is output file
#   fifth param is dump size (0=one output file, no dump)
~/sandbox/binnedPostHits/Release/binnedPostHits /home/juan/work/experiments/tmp gov2\..*ph.* 25205179 $NUM_RESULTS work/experiments/gov2.100K_OR.ph 0

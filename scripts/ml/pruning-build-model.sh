#!/usr/bin/env bash
# Juan Rodriguez jcr365@nyu.edu
#
# I use xgboost (https://github.com/dmlc/xgboost) to learn a boosting tree model.
# The reason for using xgboost is because it is as of this writing, the only
# tool I found that can handle out-of-memory ML learning on 10+ billion rows
# note: for out-of-memory computations, use filename#dtrain.cache
#
#
# Tried H2O and Spark ML. H2O can handle the set, but the resulting model will
# not compile due to a bug in their implemenation (Random Forests)
# Spark ML cannot currently handle 10M+ rows in memory, and does not do
# out of memory modeling. It would be nice if it could.
#

# parameters: 1: configuration file

if [ "$#" -ne 1 ]; then
    echo "Illegal number of parameters: build-pruning-model.sh <config file>"
    exit 1
fi

# get scritp path
CWD=`pwd`
MY_PATH="`dirname \"$0\"`"              # relative
MY_PATH="`( cd \"$MY_PATH\" && pwd )`"  # absolutized and normalized
if [ -z "$MY_PATH" ] ; then
  # error; for some reason, the path is not accessible
  # to the script (e.g. permissions re-evaled after suid)
  exit 1  # fail
fi

$MY_PATH/xgboost $1#dtrain.cache


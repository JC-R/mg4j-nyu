#!/bin/bash

#
# Run a command-line query processor for ${INDEX}.
# Optionally, provide a file containing titles.
#

INDEX=$1
TITLES=$2

if [ -z "${INDEX}" ]; then echo "Variable INDEX is not defined."; exit 1; fi;
if [ -z "${TITLES}" ]; then TITLE_ARGSTR=""; else TITLE_ARGSTR="-T ${TITLES}"; fi;

export CLASSPATH=.:$(find ../ -iname \*.jar | paste -d: -s)

java edu.nyu.tandon.query.Query ${TITLE_ARGSTR} ${INDEX}
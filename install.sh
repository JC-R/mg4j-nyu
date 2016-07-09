#!/bin/bash

##### NOTE: Tested only on ARCH Linux #####

set -e

export SH=/etc/profile.d/mg4j-nyu.sh

rm -fv ${SH}

echo "export MG4J_NYU_HOME=${PWD}" > ${SH}
echo "export MG4J_NYU_SCRIPTS=${PWD}/scripts" >> ${SH}
echo "export MG4J_NYU_CLASSPATH=${PWD}/target/mg4j-nyu-1.0.jar" >> ${SH}
echo "[Success] New ${PWD} file created."

echo "[Note] The changes will be effective upon restart of the bash environment."
echo "       To make it effective now, you might need to run:"
echo "       source ${SH}"

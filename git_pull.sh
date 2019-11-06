#!/bin/bash

set -e

REGISTRY=$1
TARGET_BANCH=$2
CURRENT_BRANCH=`git branch | grep \* | cut -d ' ' -f2`

echo "CURRENT_BRANCH: ${CURRENT_BRANCH}"

if [[ ${TARGET_BANCH} == ""]]; then
    TARGET_BANCH=${CURRENT_BRANCH}
    git pull ${REGISTRY} ${TARGET_BANCH}
else
    git checkout ${TARGET_BANCH}
    git pull ${REGISTRY} ${TARGET_BANCH}
    git checkout ${CURRENT_BRANCH}
fi


# Usage: git_pull.sh REGISTRY [BRANCH]

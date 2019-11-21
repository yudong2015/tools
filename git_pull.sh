#!/bin/bash

set -e


REGISTRY=$1
TARGET_BRANCH=$2

FILE_DIR=$(cd `dirname $0`; pwd)
${FILE_DIR}/git_status_check.sh


CURRENT_BRANCH=`git branch | grep \* | cut -d ' ' -f2`

if [[ ${TARGET_BRANCH} == "" ]]; then
    TARGET_BRANCH=${CURRENT_BRANCH}
    echo "git pull ${REGISTRY} ${TARGET_BRANCH}"
    git pull ${REGISTRY} ${TARGET_BRANCH}
else
    if [[ ${TARGET_BRANCH} == ${CURRENT_BRANCH} ]]; then
        echo "git pull ${REGISTRY} ${TARGET_BRANCH}"
        git pull ${REGISTRY} ${TARGET_BRANCH}
    else
        echo "git checkout ${TARGET_BRANCH}"
        git checkout ${TARGET_BRANCH}
        echo "git pull ${REGISTRY} ${TARGET_BRANCH}"
        git pull ${REGISTRY} ${TARGET_BRANCH}
        echo "git checkout ${CURRENT_BRANCH}"
        git checkout ${CURRENT_BRANCH}
    fi
fi


# Usage: git_pull.sh REGISTRY [BRANCH]

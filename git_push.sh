#!/bin/bash

set -e

REGISTRY=$1
TARGET_BANCH=$2
CURRENT_BRANCH=`git branch | grep \* | cut -d ' ' -f2`

if [[ ${TARGET_BANCH} == "" ]]; then
    git push -u ${REGISTRY} ${CURRENT_BRANCH}
else
    git checkout ${TARGET_BANCH}
    git push -u ${REGISTRY} ${TARGET_BANCH}
    git checkout ${CURRENT_BRANCH}
fi    


# Usage: git_push.sh REGISTRY [$BRANCH]

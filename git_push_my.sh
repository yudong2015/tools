#!/bin/bash

set -e

REGITRY="my"

TARGET_BANCH=$1
CURRENT_BRANCH=`git branch | grep \* | cut -d ' ' -f2`

if [[ ${TARGET_BANCH} == "" ]]; then
    git push -u ${REGITRY} ${CURRENT_BRANCH}
else
    git checkout ${TARGET_BANCH}
    git push -u ${REGITRY} ${TARGET_BANCH}
    git checkout ${CURRENT_BRANCH}
fi    


# Usage: git_push_my.sh [$BRANCH]

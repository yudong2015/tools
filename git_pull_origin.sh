#!/bin/bash

set -e

REGITRY="origin"

TARGET_BANCH=$1
CURRENT_BRANCH=`git branch | grep \* | cut -d ' ' -f2`

echo ${CURRENT_BRANCH}

git checkout ${TARGET_BANCH}
git pull ${REGITRY} ${TARGET_BANCH}

git checkout ${CURRENT_BRANCH}



# Usage: git_pull_origin.sh $BRANCH

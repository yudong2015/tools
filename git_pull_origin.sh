#!/bin/bash

set -e

REGITRY="origin"

TARGET_BANCH=$1
CURRENT_BRANCH=`git name-rev --name-only HEAD`

git checkout ${TARGET_BANCH}
git pull ${REGITRY} ${TARGET_BANCH}

git checkout ${CURRENT_BRANCH}



# Usage: git-pull-origin.sh $BRANCH

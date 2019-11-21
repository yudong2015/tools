#!/bin/bash

set -e

LINES=`git status|grep Changes|wc -l`
l=`echo $LINES`
if [[ $l > 0 ]];then
    echo ""
    echo -e "\033[31m There are changes not staged or be commited at current branch!!! \033[0m"
    echo ""
    git status
    exit 1
fi


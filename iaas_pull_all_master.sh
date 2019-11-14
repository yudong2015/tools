#!/bin/bash

set -e


FILE_DIR=$(cd `dirname $0`; pwd)

IAAS_HOME="/Users/yudong/workspace/qingcloud/iaas"


REPOSITORIES=("pitrix-appcenter-docs" "pitrix-bot-cluster" "pitrix-cli" "pitrix-global-bot"\
	"pitrix-billing" "pitrix-bot-frame" "pitrix-common" "pitrix-scripts"\
	"pitrix-bot-cfgmgmt" "pitrix-bots" "pitrix-frontgate" "pitrix-ws")


for REPO in ${REPOSITORIES[@]}
do
	echo "Update ${REPO} origin master..."
	cd ${IAAS_HOME}/${REPO}
	${FILE_DIR}/git_pull.sh origin master
	echo "Update ${REPO} origin master done."
	echo 
done

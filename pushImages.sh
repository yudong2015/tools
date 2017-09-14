#!/bin/bash

imgs=`docker images|grep 5000|awk '{print $1":"$2}'`

for img in ${imgs[*]}
    do
        docker push $img
    done

#!/bin/bash
 docker images -a|grep \<none\>|awk '{print $3}'|xargs docker rmi

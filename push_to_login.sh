#!/bin/bash

set -e

fs=$1

scp -i ~/Downloads/ehpc-sshkey -P 2222 -r ${PWD}/${fs} root@139.198.190.143:/root/


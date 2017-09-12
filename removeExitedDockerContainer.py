#!/usr/bin/env python
# coding=utf-8

from fabric.operations import run
from fabric.decorators import settings
from fabric.tasks import execute

CMD = "docker ps -a|grep Exited|awk '{print $1}'|xargs docker rm"
HOSTS = [
    "192.168.105.243",
    "192.168.105.244",
    "192.168.105.245"
]

def clean():
    context_dict = {
        'skip_bad_hosts': True,
        'timeout': 1800,
        'hosts': HOSTS,
        'warn_only': True,
        'user': 'dcos'
    }
    with settings(**context_dict):
        return execute(runOnRemote, CMD)

def runOnRemote(cmd):
    run(cmd)

if __name__ == '__main__':
    clean()

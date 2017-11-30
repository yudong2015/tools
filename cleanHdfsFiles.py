# coding=utf-8

import os
import traceback
from hdfs import InsecureClient


def cleanOldFilesOnHDFS(hdfsClient, hdfsPath, retainFiles):
    try:
        files = hdfsClient.list(hdfsPath)
        retainFiles = int(retainFiles)
        deletedFiles = []
        if len(files) > retainFiles:
            for f in files[:-retainFiles]:
                hdfsClient.delete(os.path.join(hdfsPath, f))
                deletedFiles.append(f)
        return deletedFiles, True
    except:
        traceback.print_exc()
        return [], False


if __name__ == '__main__':
    nameNodes = 'http://192.168.3.5:50070;http://192.168.3.6:50070'
    commonPath = '/kafka/topics/mysql_{}/partition={{}}'
    partitions = 6
    retainFileNum = 10
    hdfsClient = InsecureClient(nameNodes, user='dcos')
    topicPaths = [
            commonPath.format('linktime'),
            commonPath.format('product'),
            commonPath.format('user')
    ]
    for p in topicPaths:
        for i in range(partitions):
            print cleanOldFilesOnHDFS(hdfsClient, p.format(i), retainFileNum)
import sys, os
from kazoo.client import KazooClient


def removeNode(ZK, path):
    children = ZK.get_children(path)
    if children:
        for child in children:
            removeNode(ZK, os.path.join(path, child))
        removeNode(ZK, path)
    else:
        print 'Remove node: {}'.format(path)
        ZK.delete(path)


if __name__ == '__main__':
    zkNode = sys.argv[1]
    if len(sys.argv) > 2:
        zkHost = sys.argv[2]
        ZK = KazooClient(hosts=[zkHost])
    else:
        ZK = KazooClient(hosts=['192.168.105.247:2181'])
    ZK.start()
    removeNode(ZK, zkNode)


import sys, os, getopt
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
    zkNodes = None
    zkHost = 'master.mesos:2181'

    opts,args = getopt.getopt(sys.argv[1:], "h:n:", ["hosts=", "nodes="]);
    for opt,arg in opts:  
        if opt in ("-h", "--hosts"):
            zkHost = arg
            print "zkHost: ", zkHost
        elif opt in ('-n', '--nodes'):
            zkNodes = arg.split(',')
            print 'zkNodes: ', zkNodes
    ZK = KazooClient(hosts=[zkHost])
    ZK.start()
    if not zkNodes:
        kafkaZnodes = ['/isr_change_notification', '/admin', '/consumers', '/brokers', '/controller_epoch', '/config', '/cluster']
        zkNodes = kafkaZnodes
    for znode in zkNodes:
        removeNode(ZK, znode)
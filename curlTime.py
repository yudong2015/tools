# coding=utf-8

import time
import requests

TIME_FORMAT = '%Y-%m-%d-%H:%M:%S'
#url1 = 'http://9.0.92.3:8088/metrics'
#url2 = 'http://172.20.4.218:8088/metrics'
#curlTimeResult = '/home/dcos/jack/curlTimeResult.log'
url1 = 'http://www.baidu.com'
url2 = 'http://www.qq.com'
curlTimeResult = '/Users/jack/workspace/dcos_ops/curlTimeResult.log'



def writeFile(content, f):
    with open(f, 'a') as f:
        f.write(content)


# return used seconds
def curlMs(url):
    r = requests.get(url)
    return r.elapsed.total_seconds()


if __name__ == '__main__':
    t1 = curlMs(url1)
    t2 = curlMs(url2)
    writeFile('{}   {}   {}\n'.format(time.strftime(TIME_FORMAT), t1, t2), curlTimeResult)

#!/bin/env python
import zk
import urllib2
import json
import sys


def getinfo(bleid, jmxaddr, m, stat):
    url = 'http://%s/jolokia/exec/com.sitech.crmpd.idmm2.ble.RunTime:name=runTime/info'%jmxaddr
    s = urllib2.urlopen(url)
    o = json.load(s)
    o1 = json.loads(o['value'])
    print '====', bleid
    for o in o1:
        s = "%s\t\t%s\t\t%d\t%d\t%d"%(o['target_topic_id'], o['target_client_id'], o['total'], o['size'], o['sending'])
        if o['size'] > 0:
            m.append(bleid + '\t' + s)
        stat[0] += o['total']
        stat[1] += o['size']
        stat[2] += o['sending']
        print s
        #print json.dumps(o1, indent=4)

def getaddrs(fname):
    z = zk.ZKCli('10.162.200.211:4321,10.162.200.212:4321,10.162.200.213:4321,100.162.200.220:4321,100.162.200.221:4321')
    z.start()
    z.wait()
    base = '/idmm2/ble'
    ls = z.list(base)
    f = file(fname, 'w')
    for p in ls:
        data = z.get(base+'/'+p)
        data1 = data[0]
        addr = data1[0:data1.find(':')] + data1[data1.rfind(':'):]
        bleid = p[p.find('.')+1:]
        f.write('%s %s\n'%(bleid, addr))
    f.close()
    z.close()
    
def main(fname):
    m = []
    base = '/idmm2/ble'
    stat = [0, 0, 0]
    for line in file(fname):
        d = line.strip().split()
        if len(d) != 2:
            continue
        getinfo(d[0], d[1], m, stat)
    
    print '\n--------size > 0:'
    print 'BLEID  TOPIC_ID   CLIENT_ID  total   size   sending'
    print '---------------------------------------------------'
    for s in m:
        print '====', s
        
    print '\n=====total====='
    print '--total--size--sending'
    print '%d\t%d\t%d'%(stat[0], stat[1], stat[2])

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'getaddr':
        getaddrs('addr.list')
    main('addr.list')
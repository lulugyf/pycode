#!/bin/env python
# -*- coding: utf-8 -*-

import zk
import urllib2
import json
import sys
import os
import time

# 消息中间件BLE队列监控

def getinfo(bleid, jmxaddr, m, stat):
    url = 'http://%s/jolokia/exec/com.sitech.crmpd.idmm2.ble.RunTime:name=runTime/info'%jmxaddr
    s = urllib2.urlopen(url)
    o = json.load(s)
    o1 = json.loads(o['value'])
    print '====(%s)'%jmxaddr, bleid
    last_topic, last_client = "", ""
    for o in o1:
        s = "%s\t\t%s\t\t%d\t%d\t%d"%(o['target_topic_id'], o['target_client_id'], o['total'], o['size'], o['sending'])
        if o['size'] > 0:
            m.append(bleid + '\t' + s)
        stat[0] += o['total']
        stat[1] += o['size']
        stat[2] += o['sending']
        last_topic, last_client = o['target_topic_id'], o['target_client_id']
        print s

    # 获取dboper 的积压
    url = 'http://%s/jolokia/exec/com.sitech.crmpd.idmm2.ble.RunTime:name=runTime/lockdetail/%s/%s'%(
        jmxaddr, last_client, last_topic)
    s = urllib2.urlopen(url)
    o = json.load(s)
    o1 = json.loads(o['value'])
    dboper_block = o1['global']['blocking_db_oper']
    print '    >>DBOper_Blocking %s'%bleid, dboper_block
    #print json.dumps(o1, indent=4)

def getaddrs(zkaddr):
    z = zk.ZKCli(zkaddr)
    z.start()
    z.wait()

    f = file('.ble.list', 'w')
    f1 = file('.ble.list.1', 'w')
    base = '/idmm2/ble'
    for p in z.list(base):
        data = z.get(base+'/'+p)
        data1 = data[0]
        addr = data1[0:data1.find(':')] + data1[data1.rfind(':'):]
        bleid = p[p.find('.')+1:]
        f1.write('%s %s\n'%(bleid, data1))
        f.write('%s %s\n'%(bleid, addr))
    f.close()
    f1.close()

    f = file('.httpbroker.list', 'w')
    for p in z.list('/idmm2/httpbroker'):
        f.write(p)
        f.write('\n')
    f.close()

    f = file('.broker.list', 'w')
    for p in z.list('idmm2/broker'):
        f.write(p)
        f.write('\n')
    f.close()
    
    z.close()
    
def qmon(zkaddr):
    addrfile = '.ble.list'
    need_refresh_addrlist = False
    try:
        st = os.stat(addrfile)
        if time.time() - st.st_mtime > 3600.0:
            need_refresh_addrlist = True
    except:
        need_refresh_addrlist = True
    if need_refresh_addrlist:
        getaddrs(zkaddr)
    m = []
    
    stat = [0, 0, 0]
    for line in file(addrfile):
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
    zkaddr = '10.162.200.211:4321,10.162.200.212:4321,10.162.200.213:4321,100.162.200.220:4321,100.162.200.221:4321'
    qmon(zkaddr)

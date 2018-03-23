# -*- coding: utf-8 -*-


import urllib2
import json
import sys
import os
import time

# 消息中间件BLE队列监控

def getinfo(jmxaddr, topic, client, group, prio, msgid):
    # String msgid, String consumer_id, String dst_topic, String groupid, int priority
    url = 'http://%s/jolokia/exec/com.sitech.crmpd.idmm2.ble.RunTime:name=runTime/send/%s/%s/%s/%s/%s'%(
        jmxaddr,  msgid, client, topic, group, prio)
    s = urllib2.urlopen(url)
    o = json.load(s)
    print msgid, topic, o['value']
    

def main():
    bleaddr = {}
    for l in file('.ble.list'):
        l = l.strip()
        p = l.split()
        if len(p) != 2: continue
        bleaddr[p[0] ] = p[1]
        
    for line in file('1'):
        line = line.strip()
        p = line.split()
        if len(p) < 6:
            continue
        bleid, topic, client, group, prio, msgid = p
        addr = bleaddr[bleid]
        getinfo(addr, topic, client, group, prio, msgid)

main()

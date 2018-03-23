#coding=utf-8

import urllib
from time import strftime,localtime
import time
import json
import os
import random
from kazoo import client
import traceback
import logging
import pycurl
from cStringIO import StringIO
import random
import sys

rnd = random.Random(time.time())
'''
    start = strftime("%Y-%m-%d %H:%M:%S", localtime())
    
    c = client('20.26.20.87:59035')
    c.send('T109MarkDeal', 'Pub115', u'stop 13900')
    c.send_commit('T109MarkDeal', 'Pub115', 10, 13900)
    
    end = strftime("%Y-%m-%d %H:%M:%S", localtime())
    print 'begin time:%s,end time:%s' %(start, end)
'''

def main():
    #addr_list = getBrokerList("172.21.1.46:2181")
    #c = DMMClient('10.162.200.221:42124')
    c = DMMClient(broker_addr())
    
    msgid = c.send('Topictest7', 'Pub_test', u'stop 13900')
    if msgid is None:
        return False
    print 'message id=%s' %msgid
    c.send_commit('Topictest7', 'Pub_test', msgid, 10, 13900)

    # consume
    msgid, content = c.fetch('TopictestDest7', 'Sub_test7')
    if msgid == 0:
        print 'no more message'
        return True
    elif msgid is not None:
        print '======got', msgid, content
        c.fetch_commit('TopictestDest7', 'Sub_test7', msgid)
    else:
        print 'failed'
        return False


class DMMClient:
    def __init__(self, addr):
        self.c = pycurl.Curl()
        self.send_url = 'http://%s/SEND'%addr
        self.send_commit_url = 'http://%s/SEND_COMMIT'%addr
        self.fetch_url = 'http://%s/PULL'%addr
    
    def post(self, url, data, headers=None):
        c = self.c
        storage = StringIO()
        c.setopt(pycurl.URL, url)
        if headers is not None:
            c.setopt(pycurl.HTTPHEADER, headers)
        c.setopt(pycurl.POST, 1)
        c.setopt(pycurl.POSTFIELDS, json.dumps(data))
        c.setopt(c.WRITEFUNCTION, storage.write)
        c.perform()
        j = json.loads(storage.getvalue())
        storage.close()
        return j
    
    def send(self, topic, client, content):
        data = {"topic":topic,"client-id":client,"content":content}
        j = self.post(self.send_url, data, ['Content-Type: text/plain; charset=GBK'])
        if j['result-code'] != 'OK':
            print 'send failed', j['result-code']
            return
        print j['message-id'] , content
        return j['message-id']
        
    def send_commit(self, topic, client, messageid, priority, group):
        data = {"topic":topic,"client-id":client,"message-id":messageid,
                "priority":priority,"group":group,"custom.msg_route":"2"}
        j = self.post(self.send_commit_url, data)
        if j['result-code'] != 'OK':
            print 'send failed', j['result-code']
            return
        print 'send commit return', j['result-code']
    
    def fetch(self, topic, client, process_time=60):
        data = {'target-topic':topic,
                'client-id':client,
                'processing-time':process_time}
        j = self.post(self.fetch_url, data)
        #print '====', repr(j)
        if j['result-code'] != 'OK':
            print 'fetch failed', j['result-code']
            if j['result-code'] == 'NO_MORE_MESSAGE':
                return 0, ''
            return None, 'failed'
        msgid = j['message-id']
        content = j['content']
        print j
        return msgid, content

    def pull(self, topic, client, process_time=60,
             msgid=None, pull_code=None, desc=None):
        data = {'target-topic':topic,
                'client-id':client,
                'processing-time':process_time}
        if msgid is not None:
            data['message-id'] = msgid
            if pull_code is None:
                pull_code = 'COMMIT_AND_NEXT'
        if pull_code in ('COMMIT', 'COMMIT_AND_NEXT',
                         'ROLLBACK', 'ROLLBACK_AND_NEXT', 'ROLLBACK_BUT_RETRY'):
            data['pull-code'] = pull_code
            if pull_code == 'ROLLBACK_BUT_RETRY':
                data['retry-after'] = '60' # after 60s and retry  
        if desc is not None:
            data['code-description'] = desc
        
        j = self.post(self.fetch_url, data)
        '''if j['result-code'] != 'OK':
            print 'fetch failed', j['result-code']
            if j['result-code'] == 'NO_MORE_MESSAGE':
                return 0, ''
            return None, 'failed'
        msgid = j['message-id']
        content = j['content']
        print j
        return msgid, content'''
        return j
            
        
    def fetch_commit(self, topic, client, msgid, desc=None, replyto=None):
        data = {'target-topic':topic,'client-id':client, 'message-id':msgid,
                'pull-code':'COMMIT'}
        if desc is not None:
            data['code-description'] = desc
        if replyto is not None:
            data['reply-to'] = replyto
                
        j = self.post(self.fetch_url, data)
        print 'commit return', j['result-code']

def getBrokerList(zkURL, node_dir = '/idmm2/httpbroker'):
    logging.basicConfig()
    brokerlist = []
    print '%s' %zkURL
    try:
        zk = client.KazooClient(hosts='%s' %zkURL)
        zk.start()
        addr_list = zk.get_children(node_dir)
        print 'addr_list=%s' %addr_list
        #if len(addr_list)>0:
        #    for id in addr_list:
        #        tmp_path = node_dir + '/' + id
        #        data,stat = zk.get('%s' %tmp_path)
        #        d = data.split("//")[1]
        #        addr = d.split("/")[0]
        #        brokerlist.append(addr)
        zk.stop()
        return addr_list
    except Exception, e:
        print traceback.format_exc()

def broker_addr():
    s = file('.httpbroker.list').read().strip()
    ls = s.split('\n')
    return ls[rnd.randint(0, len(ls)-1)]


def test_once(c, bleid, pub_topic, producer, consume_topic, consumer):
    tm = 'NULL'
    try:
        msgid = c.send(pub_topic, producer, 'send_time %f'%time.time())
        if msgid is None:
            return False, tm
        print 'message id=%s' %msgid
        c.send_commit(pub_topic, producer, msgid, 10, rnd.randint(100000, 200000))

        # consume
        count = 0
        while 1:
            msgid, content = c.fetch(consume_topic, consumer)
            if msgid == 0:
                print 'no more message', count
                if count > 0:
                    return True, tm
                else:
                    return False, tm
            elif msgid is not None:
                print '======got', msgid, content
                c.fetch_commit(consume_topic, consumer, msgid)
                count += 1
                if content.startswith('send_time '):
                    tm = '%.2fms'%( (time.time()-float(content.split()[1].strip()) ) * 1000.0 )
            else:
                print 'no more message'
                break
    except Exception,x:
        print x
    return False, tm

# 把每个测试主题分别收发一条消息， 测试BLE的连通性
def test_for_ble():
    addr = broker_addr()
    print 'use broker address:', addr
    c = DMMClient(addr)
    #10000010;Topictest9;Pub_test;TopictestDest9;Sub_test9
    for line in file('test_topics'):
        line = line.strip()
        if len(line) < 10:
            continue
        ps = line.split(';');
        print '=== testing bleid:', ps[0]
        bleid, pub_topic, producer, consume_topic, consumer = ps
        ret, tm = test_once(c, bleid, pub_topic, producer, consume_topic, consumer)
        print 'RESULT ble', bleid, consume_topic, consumer, tm, ret

def test_for_broker():
    s = file('.httpbroker.list').read().strip()
    ls = s.split('\n')
    for addr in ls:
        c = DMMClient(addr)
        print "=====begin ", addr
        bleid, consume_topic, consumer = "10000010", "TopictestDest19", "Sub_test19"
        pub_topic, producer = "Topictest19", "Pub_test"
        ret, tm = test_once(c, bleid, pub_topic, producer, consume_topic, consumer)
        print 'RESULT broker', addr, consume_topic, consumer, tm, ret
        

#测试消费延迟， 连续发送1000条消息， 消息体里包含消息的生成时间
#并持续消费， 收到消息后把收到的时间和消息的生成时间相减
def bench_send(c, pub_topic, producer, limit=100):
    try:
        for i  in range(limit):
            msgid = c.send(pub_topic, producer, str(time.time()) )
            if msgid is None:
                return False
            print 'message id=%s' %msgid
            group = str(rnd.randint(100000, 200000))
            c.send_commit(pub_topic, producer, msgid, 10, group)
    except Exception,x:
        print x


def bench_fetch(c, consume_topic, consumer, limit=100):
    try:
        # consume
        count = 0
        while 1:
            msgid, content = c.fetch(consume_topic, consumer)
            
            if msgid == 0:
                print 'no more message', count
                time.sleep(0.1)
            elif msgid is not None:
                print '======got', msgid, content
                c.fetch_commit(consume_topic, consumer, msgid)
                try:
                    tm = float(content)
                    print 'cost', msgid, (time.time()-tm) * 1000.0
                except:
                    pass
                count += 1
                if count >= limit:
                    break
            else:
                print 'no more message'
                break
    except Exception,x:
        print x
    return False

def bench():
    addr = broker_addr()
    print 'use broker address:', addr
    c = DMMClient(addr)
    bleid, consume_topic, consumer = "10000010", "TopictestDest19", "Sub_test19"
    pub_topic, producer = "Topictest19", "Pub_test"

    if sys.argv[1] == 'recv':
        bench_fetch(c, consume_topic, consumer, 100)
    elif sys.argv[1] == 'send':
        bench_send(c, pub_topic, producer, 100)

# 监控ble的接管时间， 连续从一个ble上的主题上fetch 消息， 报错后直到恢复正常， 记录时间
def test_ble_transfer():
    addr = broker_addr()
    print 'use broker address:', addr
    c = DMMClient(addr)
    bleid, consume_topic, consumer = "10000010 TopictestDest19 Sub_test19".split()
    try:
        stat = 1
        # consume
        while 1:
            msgid, content = c.fetch(consume_topic, consumer)
            
            if msgid == 0:
                print 'no more message', time.time()
                if stat == 2:
                    break
                time.sleep(0.5)
            elif msgid is not None:
                print '======got', msgid, content
                c.fetch_commit(consume_topic, consumer, msgid)
                print 'got message', time.time()
                if stat == 2:
                    break
            else:
                print 'error', time.time()
                stat = 2
                time.sleep(0.5)

    except Exception,x:
        print x
    return False

if __name__ == '__main__':
    test_for_ble()
    test_for_broker()
    #bench()
    #test_ble_transfer()

    

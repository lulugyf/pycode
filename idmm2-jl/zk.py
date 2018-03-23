#coding=gbk

from kazoo.client import KazooClient, KazooState
import kazoo
import logging
import time
import Queue
import os
import socket


class ZKCli:
    def __init__(self, hosts):
        logging.basicConfig()
        self.hosts = hosts
        zk = KazooClient(hosts=hosts)
        zk.add_listener(self.my_listener)
        self.zk = zk
        self.q = Queue.Queue()
        
    def start(self):
        self.zk.start()

    def stop(self):
        self.zk.stop()
        
    def __ck_path__(self, path):
        pp = path.split('/')
        for i in range(1, len(pp)):
            p = '/'.join(pp[0:i+1])
            if not self.zk.exists(p):
                self.zk.create(p, value=b'')
                
    def list(self,path):
        return self.zk.get_children(path)
    
    def get(self, path):
        return self.zk.get(path)
    
    # 创建临时节点， 成功返回True， 已经存在返回 False， 其余则失败
    def create(self, path, value):
        zk = self.zk
        try:
            r = self.zk.create(path, value=value, ephemeral=True)
            print '==1', r
            return True
        except kazoo.exceptions.NoNodeError,e:
            #check parent path
            ppath = path[0:path.rfind('/')]
            self.__ck_path__(ppath)
            print 'check parent path:', ppath
            try:
                self.zk.create(path, value=value, ephemeral=True)
                return True
            except kazoo.exceptions.NodeExistsError,e:
                print 'exists again...'
                return False
            except:
                print 'unknow error again'
                return False
        except kazoo.exceptions.NodeExistsError,e:
            print 'exists'
            return False
        except:
            print 'unknow error'
            return False
        
    def my_listener(self, state):
        if state == KazooState.LOST:
            print '==LOST'
            # Register somewhere that the session was lost
        elif state == KazooState.SUSPENDED:
            print '==SUSPENDED'
            # Handle being disconnected from Zookeeper
        elif state == KazooState.CONNECTED:
            print "==CONNECTED"
            self.q.put('h')
        else:
            print '==state', state
            # Handle being connected/reconnected to Zookeeper

    def wait(self):
        h = self.q.get()
        
    def close(self):
        self.zk.stop()
        self.zk.close()
        
def checkStartInfo(zkAddr, taskname, zkRoot='/db_sync_all'):
    zk = ZKCli(zkAddr)
    zk.start()
    zk.wait()
    
    # get hostname and processid
    val = "%s--%d"%(socket.gethostname(), os.getpid())
    if zkRoot[0] != '/':
        zkRoot = '/' + zkRoot
    while True:
        if zk.create(zkRoot+'/'+taskname, val):
            break
        print 'already exists, sleep'
        time.sleep(5.0)
        
    
def main():
    zk = ZKCli('172.21.1.36:52181')
    zk.start()
    
    print "waitting..."
    zk.wait()
    print 'connected'
    print 'created:', zk.create('/test1/temp1', b'123')
    
    
    time.sleep(120.0)
    zk.stop()


if __name__ == '__main__':
    #main()
    checkStartInfo('127.0.0.1:2181', 'hello1')
    while True:
        print 'working loop', time.time()
        time.sleep(10.0)

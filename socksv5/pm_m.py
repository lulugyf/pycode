#!/usr/bin/python
# This is a simple port-forward / proxy, written using only the default python
# library. If you want to make a suggestion or fix something you can contact-me
# at voorloop_at_gmail.com
# Distributed over IDC(I Don't Care) license
from gevent import socket
from gevent import select
import gevent
import time
import sys
import socks
import threading
 
# Changing the buffer_size and delay, you can improve the speed and bandwidth.
# But when buffer get to high or delay go too down, you can broke things
buffer_size = 4096
delay = 0.0001
log = None
 
def createConnection(addr, proxy=None):
    if proxy is None:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    else:
        s = socks.socksocket()
        s.set_proxy(socks.SOCKS5, proxy[0], proxy[1])
    s.connect(addr)
    return s


class TheServer_gv:
    input_list = []
    channel = {}

    # [(listport, remote_host, remote_port),]
    def __init__(self, ports, proxy=None):
        self.ports = ports
        self.proxy = proxy
    def main_loop(self):
        print '== proxy address', proxy_addr
        greenlets_list = []
        for pp in self.ports:
            l = gevent.spawn(self.listen, pp, self.proxy)
            greenlets_list.append(l)
        gevent.joinall(greenlets_list)

    # (listport, remote_host, remote_port)
    def listen(self, pp, proxy_addr):
        target_addr = (pp[1], pp[2])
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('', pp[0]))
        s.listen(20)
        print 'listen on %s for %s:%d' % (pp[0], pp[1], pp[2])

        while 1:
            try:
                clientsock, clientaddr = s.accept()
            except:
                break
            print clientaddr, "has connected"
            remotesock = createConnection(target_addr, proxy_addr)
            gevent.spawn(self.data_tran, clientsock, remotesock)
            gevent.spawn(self.data_tran, remotesock, clientsock)
        s.close()

    def data_tran(self, s1, s2):
        while 1:
            try:
                data = s1.recv(1024)
                if len(data) == 0:
                    break
                s2.sendall(data)
            except:
                break
        s1.close()
        s2.close()


class TheServer:
    input_list = []
    channel = {}
 
    # [(listport, remote_host, remote_port),]
    def __init__(self, ports, proxy=None):
        self.running = True
        self.proxy = proxy
        self.servers = {}
        for pp in ports:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('', pp[0]))
            s.listen(20)
            self.servers[s] = (pp[1], pp[2])
            print 'listen on %s for %s:%d'%(pp[0], pp[1], pp[2])
        #self.forward_to = forward_to
 
    def main_loop(self):
        #self.input_list.append(self.server)
        print 'service started proxy[%s]'%repr(self.proxy)
        for k in self.servers.keys():
            self.input_list.append(k)
        channel = self.channel
        while self.running:
            time.sleep(delay)
            ss = select.select
            inputready, outputready, exceptready = ss(self.input_list, [], [], 5.0)
            for s in inputready:
                if self.servers.has_key(s):
                    self.on_accept(s)
                    break
 
                try:
                    data = s.recv(buffer_size)
                except Exception,ex:
                    print ex
                    self.on_close(s)
                    continue
                if len(data) == 0:
                    self.on_close(s)
                    break
                else:
                    try:
                        channel[s].sendall(data)
                    except:
                        self.on_close(s)
 
    def on_accept(self, s):
        addr = self.servers.get(s)
        forward = createConnection(addr, self.proxy)
        clientsock, clientaddr = s.accept()
        if forward:
            print clientaddr, "has connected"
            self.input_list.append(clientsock)
            self.input_list.append(forward)
            self.channel[clientsock] = forward
            self.channel[forward] = clientsock
        else:
            print "Can't establish connection with remote server.",
            print "Closing connection with client side", clientaddr
            clientsock.close()
 
    def on_close(self, s):
        print s.getpeername(), "has disconnected"
        #remove objects from input_list
        out = self.channel[s]
        self.input_list.remove(s)
        self.input_list.remove(out)
        # close the connection with client
        self.channel[out].close()  # equivalent to do self.s.close()
        # close the connection with remote server
        self.channel[s].close()
        # delete both objects from channel dict
        del self.channel[out]
        del self.channel[s]

class Service:
    def __init__(self):
        pass

    def start(self):
        self.server = TheServer(port_list, proxy_addr)

        log.info( "---starting..." )
        try:
            self.th = threading.Thread(target=self.forever, args=())
            self.th.daemon = False
            self.th.start()
            log.info( "---started..." )
        except Exception,ex:
            log.error("---failed: %s"%ex)

    def stop(self):
        self.server.running = 0
        self.th.join(10.0)

    def forever(self):
        log.info("begin...")
        try:
            self.server.main_loop()
        except Exception,ex:
            log.error( "main_loop failed %s" % ex)
            
        log.info("exited!!!")

port_list = [(5510, '10.95.242.147', 1522),  #centdb 10.95.242.147:1522   DBOFFONADM/dbaccopr200606@centdb
            (5511, '10.112.2.22',   1521),  #cms_shengchan  dbsapopr/D9Zx3Zbu!4l_@ONSERDB1 10.112.2.22:1521
            (5512, '10.105.2.18',   1521),  #offondb  offon/opr_offon292@offondb   10.105.2.18:1521
            (5513, '10.105.2.30',   1521),  #onoffdb1  offon/Crmpd!06@onoffdb
            (5514, '10.105.2.33',   1521),  #onoffdb2  offon/Crmpd!06@onoffdb
            (5515, '10.95.242.146',   1522),  #          tran/_m9n^Fh4@CRMATDB
            (5522, '10.113.183.36', 22),
             (8861, '10.113.183.36', 8861),
    ]
#proxy = ("10.109.2.179", 7070)
proxy_addr = ("10.95.238.70", 10560)

if __name__ == '__main__':
    server = TheServer_gv(port_list, proxy_addr)
    try:
        server.main_loop()
    except KeyboardInterrupt:
        print "Ctrl C - Stopping server"
        sys.exit(1)

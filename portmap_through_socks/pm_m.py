#!/usr/bin/python
# This is a simple port-forward / proxy, written using only the default python
# library. If you want to make a suggestion or fix something you can contact-me
# at voorloop_at_gmail.com
# Distributed over IDC(I Don't Care) license
import socket
import select
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
        #self.forward_to = forward_to
 
    def main_loop(self):
        #self.input_list.append(self.server)
        for k in self.servers.keys():
            self.input_list.append(k)
        while self.running:
            time.sleep(delay)
            ss = select.select
            inputready, outputready, exceptready = ss(self.input_list, [], [], 5.0)
            for self.s in inputready:
                if self.servers.has_key(self.s):
                    self.on_accept()
                    break
 
                try:
                    data = self.s.recv(buffer_size)
                except Exception,ex:
                    print ex
                    self.on_close()
                    continue
                if len(data) == 0:
                    self.on_close()
                    break
                else:
                    self.on_recv(data)
 
    def on_accept(self):
        addr = self.servers.get(self.s)
        forward = createConnection(addr, self.proxy)
        clientsock, clientaddr = self.s.accept()
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
 
    def on_close(self):
        print self.s.getpeername(), "has disconnected"
        #remove objects from input_list
        self.input_list.remove(self.s)
        self.input_list.remove(self.channel[self.s])
        out = self.channel[self.s]
        # close the connection with client
        self.channel[out].close()  # equivalent to do self.s.close()
        # close the connection with remote server
        self.channel[self.s].close()
        # delete both objects from channel dict
        del self.channel[out]
        del self.channel[self.s]
 
    def on_recv(self, data):
        # here we can parse and/or modify the data before send forward
        #print data
        self.channel[self.s].send(data)

		
class Service:
    def __init__(self):
        pass

    def start(self):
        port_list = [(9090, '10.95.242.147', 1522), # cms test
                     (9091, '10.112.2.22',   1521), # cms sapdb
                     (9092, '10.105.2.18',   1521), # offondb
					 (9093, '10.105.2.30',   1521), # onoffdb (onoff1)
					 (9094, '10.105.2.33',   1521), # onoffdb (onoff2)
					 (7001, '10.95.242.177', 22),   # ssh crmtux1 / Stq@2014
                     ]
        proxy = ("10.109.2.179", 7070)
        self.server = TheServer(port_list, proxy)

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


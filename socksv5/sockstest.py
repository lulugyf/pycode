import sys
#sys.path.append("..")
import socks
import socket

PY3K = sys.version_info[0] == 3

if PY3K:
    import urllib.request as urllib2
else:
    import sockshandler
    import urllib2
	
def socket_SOCKS5_test():
    s = socks.socksocket()
    s.set_proxy(socks.SOCKS5, "10.95.238.57", 7070)
    s.connect(("10.105.2.16", 23))
    #s.sendall(raw_HTTP_request())
    print s.recv(2048)
    s.close()
if __name__ == '__main__':
    socket_SOCKS5_test()

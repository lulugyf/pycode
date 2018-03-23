import pm_m as px
import sys

if __name__ == '__main__':
    server = px.TheServer_gv(px.port_list, px.proxy_addr)
    try:
        server.main_loop()
    except KeyboardInterrupt:
        print "Ctrl C - Stopping server"
        sys.exit(1)

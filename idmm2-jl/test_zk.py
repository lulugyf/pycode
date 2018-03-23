#coding=utf-8
import mon
import time
import web
import os

# ol-old_list   nl-new_list
def wfunc(ol, nl):
    #print 'wfunc...', type(e), e.state, e.path, e.type, e.count(), e.index()
    #print dir(e)
    #print repr(e)
    print 'ADD', list(set(nl) - set(ol))
    print "RMV", list(set(ol) - set(nl))

def main():
    zk = mon.ZKCli('127.0.0.1:2181')
    zk.start()

    print "connecting..."
    zk.wait()
    print 'connected'

    # watching the change of path
    zk.list('/idmm2/ble', wfunc)


    print 'waiting...'
    time.sleep(120.0)

    zk.close()



'''
curl -d guanyf --header "file-name: myfile" http://localhost:8080/

upload file with blocks:
1 GET ->  filename:???   filelength: ???
  <-  result: yes|no  start: 0|n|-1          if -1, file upload finished
2 POST ->  filename:???   end: yes|no
  ->  block of file data
  <-  result: yes|no

'''

urls = (
    '/', 'index',
    '/upload', 'upload',
    '/download', 'download'
)
class index:
    def GET(self):
        return "Hello, world!"
    def POST(self):
        env = web.ctx.env
        print 'post data', repr(web.data())
        print env.get('HTTP_FILENAME', '--')
        return "Hello "+web.data()

class upload:
    def GET(self):
        env = web.ctx.env
        fname = env.get("HTTP_FILENAME", None)
        flen = int(env.get('HTTP_FILELENGTH', 0))
        fpath = 'data/%s'%fname
        web.header('RESULT', 'yes')
        if os.path.exists(fpath):
            web.header('START', "-1")
        else:
            fpath = fpath + '._pt'
            if os.path.exists(fpath):
                st = os.stat(fpath)
                if st.st_size < flen:
                    web.header('START', str(st.st_size))
                else:
                    os.rename(fpath, fpath[:-4])
                    web.header('START', "-1") # finished
            else:
                web.header('START', "0")
        return ''
    def POST(self):
        env = web.ctx.env
        fname = env.get("HTTP_FILENAME", None)
        fpath = 'data/%s._pt' % fname
        f = open(fpath, 'ab')
        data = web.data()
        f.write(data)
        f.close()
        flen = os.path.getsize(fpath)
        l = len(data)
        print '--', flen-l, l
        if env.get('HTTP_END', 'no') == 'yes':
            os.rename(fpath, fpath[:-4])
        web.header('RESULT', 'yes')
        return ''

'''
download file with resume:
1 GET -> filename: ??? start: ???
  <- result: yes|no  end: yes|no
  <- block of file data
'''
class download:
    def GET(self):
        blocksize = 40960
        env = web.ctx.env
        fname = env.get("HTTP_FILENAME", None)
        pos = int(env.get('HTTP_START', 0))
        fpath = "data/%s" %fname
        if not os.path.exists(fpath):
            web.header("RESULT", "file not exists")
            return ''
        flen = os.path.getsize(fpath)
        if pos >= flen:
            web.header("RESULT", "yes")
            web.header("END", "yes")
            return ''
        if flen - pos <= blocksize:
            web.header("END", "yes")
            blocksize = flen - pos
        web.header("RESULT", "yes")
        f = open(fpath, 'rb')
        f.seek(pos)
        data = f.read(blocksize)
        f.close()
        return data



if __name__ == '__main__':
    #main()
    try:
        os.mkdir('data')
    except: pass
    app = web.application(urls, globals())
    app.run()
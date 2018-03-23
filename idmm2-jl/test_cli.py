#coding=utf-8

import pycurl
import os
import time
from cStringIO import StringIO

def header2map(s):
    m = {}
    for l in s.split('\r\n')[1:]:
        p = l.find(':')
        if p > 0:
            m[l[:p].strip()] = l[p+1:].strip()
    return m
def upload_get(c, url, filename):
    storage = StringIO()
    headers = StringIO()
    c.setopt(pycurl.URL, url)
    flen = os.path.getsize(filename)
    fname = filename
    if fname.find('/'): fname = fname[fname.rfind('/') + 1:]
    c.setopt(pycurl.HTTPHEADER, ['FILENAME: %s' % fname, 'FILELENGTH: %d'%flen])
    c.setopt(pycurl.POST, 0)
    c.setopt(pycurl.WRITEFUNCTION, storage.write)
    c.setopt(c.HEADERFUNCTION, headers.write)
    c.perform()

    r = header2map(headers.getvalue())
    headers.close()
    storage.close()

    if r.get('RESULT', 'no') != 'yes':
        print "refused !!"
        return -1
    print '--RESULT', r.get('RESULT', '--')
    return int(r.get("START", "0"))

def upload_post(c, url, filename, pos, blocksize):
    fname = filename
    if fname.find('/'): fname = fname[fname.rfind('/')+1:]
    flen = os.path.getsize(filename)
    if pos >= flen:
        return 0

    f = open(filename, 'rb')
    f.seek(pos)

    storage = StringIO()
    headers = StringIO()
    c.setopt(pycurl.URL, url)
    if flen-pos <= blocksize:
        blocksize = flen-pos
        c.setopt(pycurl.HTTPHEADER, ['FILENAME: %s' % fname, 'END: yes', 'Content-Length: %d'%blocksize,
                                     'Content-Type: application/x-www-form-urlencoded'])
    else:
        c.setopt(pycurl.HTTPHEADER, ['FILENAME: %s' % fname, 'Content-Length: %d'%blocksize,
                                     'Content-Type: application/x-www-form-urlencoded'] )
    c.setopt(pycurl.POST, 1)
    c.setopt(pycurl.POSTFIELDSIZE, blocksize)
    c.setopt(c.WRITEFUNCTION, storage.write)
    c.setopt(c.HEADERFUNCTION, headers.write)
    c.setopt(pycurl.READFUNCTION, f.read)
    c.perform()

    #print '===', headers.getvalue()
    r = header2map(headers.getvalue())
    print '--pos: ', pos, '--blocksize', blocksize, 'RESULT=', r.get("RESULT", '--')

    f.close()
    storage.close()
    headers.close()
    return blocksize

def upload(url, fname):
    blocksize = 40960
    c = pycurl.Curl()
    #url = 'http://127.0.0.1:8080/upload'
    #fname = 'pexpect.zip'
    pos = 0
    while 1:
        try:
            pos = upload_get(c, url, fname)
            if pos < 0:
                print "pos < 0", pos, "terminated!"
                return
            break
        except:
            time.sleep(1.0)
    while 1:
        try:
            slen = upload_post(c, url, fname, pos, blocksize)
            if slen <= 0:
                break
            pos = pos + slen
        except:
            print '-resume--'
            while 1:
                try:
                    pos = upload_get(c, url, fname)
                    if pos < 0:
                        print "exp pos < 0", pos, "terminated!"
                        return
                    break
                except:
                    time.sleep(1.0)

# return is-end, is-error
def download_get(c, url, filename):
    if os.path.exists(filename):
        print "already finished!"
        return True, False #write end
    fpath = "%s._pt" % filename
    pos = 0
    if os.path.exists(fpath):
        pos = os.path.getsize(fpath)

    headers = StringIO()
    c.setopt(pycurl.URL, url)
    fname = filename
    if fname.find('/'): fname = fname[fname.rfind('/') + 1:]
    c.setopt(pycurl.HTTPHEADER, ['FILENAME: %s' % fname, 'START: %d'%pos])
    c.setopt(pycurl.POST, 0)
    f = open(fpath, 'ab')
    c.setopt(pycurl.WRITEFUNCTION, f.write)
    c.setopt(pycurl.HEADERFUNCTION, headers.write)
    c.perform()
    f.close()

    r = header2map(headers.getvalue())
    headers.close()
    ret = r.get('RESULT', '--')
    if ret != 'yes':
        print "refused !!",  ret
        return False, True
    end = r.get("END", "--")
    print '--RESULT', ret, '--END', end
    if end == "yes":
        os.rename(fpath, fpath[:-4])
        return True, False
    return False, False

def download(url, fpath):
    c = pycurl.Curl()
    while 1:
        try:
            end, err = download_get(c, url, fpath)
            if err:
                print 'error occurs, terminated'
                return
            if end:
                break
        except:
            print '---resume---'
            time.sleep(1.0)


if __name__ == '__main__':
    url = 'http://127.0.0.1:8080/upload'
    fname = 'pexpect.zip'
    upload(url, fname)

    url = 'http://127.0.0.1:8080/download'
    fpath = 'tmp/pexpect.zip'
    try: os.mkdir('tmp')
    except: pass
    download(url, fpath)

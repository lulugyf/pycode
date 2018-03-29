
#encoding=utf-8

import os
import time
import tushare as ts
import io
import datetime
from multiprocessing import Pool


# 清理文件数据(for k_day): 2018-01-31 19.91 19.91 19.91 19.91 4051.0 603056  => 20180131 19.91 19.91 19.91 19.91 4051.0
def format_files_day(basedir = 'e:/stock/list11'):
    for f in os.listdir(basedir):
        fpath = "%s/%s"%(basedir, f)
        fpath_tmp = fpath + "_"
        fo = open(fpath_tmp, "w")
        count = 0
        for l in open(fpath):
            r = l.strip().split()
            r[0] = r[0].replace('-', '')
            del r[-1]
            fo.write(" ".join(r)); fo.write("\n")
            count += 1
        fo.close()
        os.remove(fpath)
        os.rename(fpath_tmp, fpath)
        print("file %s  count: %d"%(f, count))

# 清理文件数据(for 60min): "2017-03-02 11:30" 16.17 16.18 16.19 16.09 12804.0 000019  => 201703021130 16.17 16.18 16.19 16.09 12804.0
def format_files_60(basedir = 'e:/stock/60min'):
    for f in os.listdir(basedir):
        fpath = "%s/%s"%(basedir, f)
        fpath_tmp = fpath + "_"
        fo = open(fpath_tmp, "w")
        count = 0
        for l in open(fpath):
            r = l.strip().replace("\"", "").split()
            r[1] = r[0].replace('-', '') + r[1].replace(":", "")
            fo.write(" ".join(r[1:-1])); fo.write("\n")
            count += 1
        fo.close()
        os.remove(fpath)
        os.rename(fpath_tmp, fpath)
        print("file %s  count: %d"%(f, count))

def __one_stock_hday(a):
    code, timeToMarket, basedir = a
    f_file = "%s/%s.txt"%(basedir, code)
    print("begin %s %s exists: %s"%(code, timeToMarket, os.path.exists(f_file)))
    #return

    if len(timeToMarket) < 8:
        return
    year = int(timeToMarket[:4])
    if os.path.exists(f_file):
        done = False
        with open(f_file, "rb") as fh:
            fh.seek(-100, 2)
            last = fh.readlines()[-1]
            #print(repr(last), type(last))
            if last.startswith(b"2018"):
                done = True
            else:
                year = int(last[:4]) + 1
                print("continue with year: %d" % year)
        if done:
            print("exists, skip %s"%code)
            return

    while year < 2019:
        df = ts.get_k_data(code, autype='hfq', start='%d-01-01'%year, end="%d-01-01"%(year+1))
        print("     %s count=%d"%(year, df.shape[0]))
        year += 1
        df.to_csv(f_file, mode='a', index=None, header=None, sep=' ')
    print("done with %s pid=%d"%(code, os.getpid()))

# 下载日线, 从上市时间开始的
# TODO 每天下载追加的新数据
def downtushare_hday():
    import os
    basedir = "e:/stock/list11"
    f_slist = 'e:/stock/stocklist.txt'
    # df = ts.get_stock_basics()
    # df.to_csv(f_slist, sep=',', header=None)  # mode='a', index=None
    try: os.mkdir(basedir)
    except: pass

    slist = [(l.split(',')[0], l.split(',')[15], basedir) for l in open(f_slist, encoding='gbk')]
    with Pool(8) as p:
        p.map(__one_stock_hday, slist)
    # for a in slist: __one_stock(a)

def __one_stock_60(a):
    code, timeToMarket, basedir, month_list = a
    f_file = "%s/%s.txt"%(basedir, code)
    print("begin %s %s exists: %s"%(code, timeToMarket, os.path.exists(f_file)))

    if os.path.exists(f_file):
        print("exists, skip %s"%code)
        return

    for i in range(len(month_list)-1):
        # print("%s  --  %s"%(month_list[i], month_list[i+1]))
        df = ts.get_k_data(code, autype='hfq', ktype="60", start=month_list[i], end=month_list[i+1])
        print("     %s count=%d"%(month_list[i], df.shape[0]))
        df.to_csv(f_file, mode='a', index=None, header=None, sep=' ')
    print("done with %s pid=%d"%(code, os.getpid()))

# 下载60分钟线, 从2016-01-01 开始
def downtushare_60():
    import os
    basedir = "e:/stock/60min"
    f_slist = 'e:/stock/stocklist.txt'
    try:
        os.mkdir(basedir)
    except:
        pass

    month_list = ['2016-%02d-01' % i for i in range(1, 13)]
    month_list.extend(['2017-%02d-01' % i for i in range(1, 13)])
    month_list.extend(['2018-%02d-01' % i for i in range(1, 5)])
    # slist = [(l.split(',')[0], l.split(',')[15], basedir, month_list) for l in open(f_slist, encoding='gbk')]
    # with Pool(8) as p:
    #     p.map(__one_stock_60, slist)
    __one_stock_60(('600212', '', basedir, month_list))

###############
################# 从 tushare 增量下载日线数据
def lastline(fpath):
    with open(fpath, "rb") as f:
        n = 1024
        if os.path.getsize(fpath) > n:
            f.seek(-n, 2)
        return f.readlines()[-1]
def dateadd(sdate, n):
    sdate = sdate.decode()
    d = datetime.datetime.strptime(sdate, '%Y%m%d')
    d1 = d + datetime.timedelta(days=n)
    return d1.strftime("%Y-%m-%d")

import time
def __ts_down_increasely(fpath):
    fname = fpath[fpath.rfind('/')+1:]
    if len(fname) != 10:
        print("not a properly file[%s]"%fname)
        return
    code = fname[-10: -4]
    #fpath = "%s/%s" %(basedir, fname)
    l = lastline(fpath)
    start = dateadd(l[:8], 1)
    if time.strftime("%Y%m%d") == l[:8]:
        print("today skip1 %s"%code); return
    # today = time.strftime("%Y-%m-%d")
    # if today == start:
    #     print("%s today, skip"%code)
    #     return
    df = ts.get_k_data(code=code,autype='hfq',ktype='D',start=start)
    out = io.StringIO()
    df.to_csv(out, header=None, index=None, sep=' ')
    i = 0
    with open(fpath, "a") as fo:
        out.seek(0, 0)
        for line in out.readlines():
            r = line.strip().split()
            r[0] = r[0].replace('-', '')
            fo.write(" ".join(r[:-1]))
            fo.write('\n')
            i+=1
        out.close()
    print("%s count %d begin %s"%(code, i, start))

def ts_down_increasely(basedir='e:/stock/list11'):
    flist = ["%s/%s"%(basedir, fname) for fname in os.listdir(basedir)]
    with Pool(8) as p:
        p.map(__ts_down_increasely, flist)

if __name__ == '__main__':
    #downtushare_60()
    #format_files_60()
    ts_down_increasely()
    pass

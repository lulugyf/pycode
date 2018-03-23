#encoding=utf-8

import struct
import os
import numpy as np
from scipy.ndimage.interpolation import shift
from multiprocessing import Pool
#import tushare as ts

class lday:
    @staticmethod
    def unpack1(s):
        ldate, lopen, lhigh, llow, lclose, lmoney, lvolume, lbefore = struct.unpack('IIIIIfII', s)
        #print(ldate, lopen, lhigh, llow, lclose, lvolume)
        return ldate, lopen, lhigh, llow, lclose, lmoney, lvolume, lbefore

    @staticmethod
    def unpack(s):
        a = s.strip().split()
        return a[0], int(100.0*float(a[1])), int(100.0*float(a[3])), int(100.0*float(a[4])), int(100.0*float(a[2])), 0, int(100.0*float(a[5])), 0





# 从找到的弱势时间点画前后的曲线:
def weaks_lines(before=67, after=10):
    basedir = "e:/stock/list11"
    out = open("e:/stock/weaklines.txt", "w")
    for line in open("e:/stock/11h"):
        r = line.strip().split()
        code, date = r[0][:6], int(r[1])-1
        fpath = "%s/%s.txt"%(basedir, code)
        arr = np.loadtxt(fpath, dtype=float)
        for i in range(arr.shape[0]):
            if arr[i, 0] > date:
                v = i
                break
        ret = arr[v-before:v+after, 4]
        x = ret[0]
        r = (ret - x) / x * 100.0
        s = np.array_str(r, max_line_width=1024, precision=8)
        out.write(s[1:-1])
        out.write('\n')
    out.close()

def figureit(fpath='e:/stock/weaklines.txt', ymin=-100, ymax=100, count=30):
    # 绘制图形, 从k线生成的数据那个， 聚类后其中的一类， split_lines() 生成的文件
    # 由于数据量大， 从里面随机抽取n个绘制
    import sys, numpy as np
    import random
    import matplotlib.pyplot as plt

    lines = open(fpath).readlines()
    rng = [i for i in range(len(lines))]
    random.shuffle(rng)
    for i in range(count):
        l = lines[rng[i]]
        dc = [ float(x) for x in l.strip().split() ]
        plt.plot(dc)
    #plt.plot(centroid, linewidth=5.0, linestyle='--', color='red')
    plt.ylim(ymin, ymax)
    #plt.legend()
    plt.show()

import time
def __one_stock(a):
    import tushare as ts
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

def downtushare():
    import os
    basedir = "e:/stock/list11"
    f_slist = 'e:/stock/stocklist.txt'
    # df = ts.get_stock_basics()
    # df.to_csv(f_slist, sep=',', header=None)  # mode='a', index=None
    try: os.mkdir(basedir)
    except: pass

    slist = [(l.split(',')[0], l.split(',')[15], basedir) for l in open(f_slist, encoding='gbk')]
    with Pool(8) as p:
        p.map(__one_stock, slist)
    # for a in slist: __one_stock(a)

# 清理文件数据: 2018-01-31 19.91 19.91 19.91 19.91 4051.0 603056  => 20180131 19.91 19.91 19.91 19.91 4051.0
def format_files():
    basedir = 'e:/stock/list11'
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



# 寻找后续走势情况中, H = Hn 这一段时间内, 最低价/周期/H
def check_next(dc, idx, nH, cPrice, nMin):
    HH = np.max(dc[idx-nMin:idx+1, 3])
    buyprice = (cPrice - HH) / HH * 100.0
    for i in range(idx+1, dc.shape[0]):
        H = dc[i, 3]  # 当前最高价
        if H + 0.01 >= np.max(dc[i-nH: i+1, 3]): # H 碰到 nH 最高价
            return "    circle %d min %.2f%% end %.2f%%  buy: %.2f%%"%(
                i-idx, (np.min(dc[idx+1:i+1, 4])-cPrice)/cPrice * 100.0, (H-cPrice)/cPrice * 100.0,
                buyprice
            )
    return "    opening: min=%.2f%%  buy: %.2f%%" % ((np.min(dc[idx+1:, 4])-cPrice)/cPrice * 100.0, buyprice)

# 连续 6*11+1 天下跌, 然后下跌达到 11h/1.3
# 文件数据格式 date open close high low
def one_file(args): # fpath, nH=11, loop=6, ratio=1.3):
    fpath, nH, loop, ratio, mindate, outf_name = args
    dc = np.loadtxt(fpath, dtype=float)
    if dc.shape[0] < nH * loop + 10:
        return 0
    fcode = fpath[-10:-4]

    nMin = nH * loop + 1  # 最少周期数
    nCur = 0   #当前连续下跌的天数, 一旦有上涨的, 就清零

    lastH = np.max(dc[:nH, 3])
    for i in range(nH + 1, dc.shape[0]):
        if dc[i, 0] < mindate:
            continue
        L = dc[i, 4]
        cH = np.max(dc[i - nH: i, 3])  # 当前的 11H
        if cH <= lastH+0.02:
            nCur += 1  # 最高价累计跌的天数
        else:
            nCur = 0
        lastH = cH

        if nCur >= nMin:
            if L * ratio < cH:
                result = check_next(dc, i, nH, cH/ratio, nMin)
                open(outf_name, "a").write(
                "%s %s %d %s\n"%(fcode, dc[i, 0], nCur, result))
                nCur = 0

def weaks():
    #one_file(('e:/stock/list11/600806.txt', 19,6,1.3))
    #return

    basedir = "e:/stock/list11"
    nH, loop, ratio = 19, 5, 1.56
    outf_name = "e:/stock/w_%d-%d-%.2f.txt" % (nH, loop, ratio)
    filelist = [("%s/%s"%(basedir, fname), nH, loop, ratio, 20100101, outf_name) for fname in os.listdir(basedir)]
    #for args in filelist: one_file(args)
    Pool(8).map(one_file, filelist)

r'''
# set path=d:\dev\Anaconda3\Scripts;d:\dev\Anaconda3;C:\WINDOWS\system32;C:\WINDOWS;D:\dev\git\usr\bin
# set QT_PLUGIN_PATH=d:\dev\Anaconda3\Library\plugins  matplotlib failed, need this

import sys
sys.path.insert(0,'d:/worksrc/pycode/stock')
import weak_11h as h11
from importlib import reload
h11.downtushare1()
'''
if __name__ == '__main__':
    pass
    #downtushare()
    weaks()
    #one_file(r'E:\stock\new_yhzq_v21\vipdoc\sh\lday\sh600212.day')

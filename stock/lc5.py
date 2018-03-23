#encoding=utf-8
import struct

import matplotlib.pyplot as plt
import math
import sys
import os
from scipy.ndimage.interpolation import shift
import numpy as np
'''
https://gist.github.com/erogol/7946246
https://github.com/erogol/KLP_KMEANS/blob/master/klp_kmeans.py

http://www.ilovematlab.cn/thread-226577-1-1.html  数据格式说明
5分钟k线 格式
typedef struct
{
 WORD date;  //日期(月、日) 
                  year=floor(num/2048)+2004;
                  month=floor(mod(num,2048)/100);
                  day=mod(mod(num,2048),100);
 WORD time;  //时间:取得数575表示9:35,取得数900表示15:00，转换方法为：575/60=9.583333333
     //整数为小时，小数9.5833333*60=35即为分钟数 

 FLOAT open; //开盘
 FLOAT high; //最高
 FLOAT low; //最低
 FLOAT close; //收盘
 FLOAT money; //成交金额
 DWORD volume; //成交量
 FLOAT temp; //保留
}TDX_MIN5;
typedef struct
{
 DWORD date; //日期(年、月、日)
 DWORD open; //开盘
 DWORD high; //最高
 DWORD low; //最低
 DWORD close; //收盘
 FLOAT money; //成交金额
 DWORD volume; //成交量
 DWORD before; //前收盘
}TDX_DAY;

'''

class lc5:
    def __init__(self):
        pass
    @staticmethod
    def unpack(s):
        ldate, ltime, lopen, lhigh, llow, lclose, lmoney, lvolume, ltemp = struct.unpack('HHfffffIf', s)
        _y = math.floor(ldate / 2048) + 2004
        _m = math.floor(ldate % 2048 / 100)
        _d = ldate % 2048 % 100
        ldate = int(_y * 10000 + _m * 100 + _d)
        print(ldate, ltime, lopen, lhigh, lclose)
        return ldate, ltime, lopen, lhigh, llow, lclose, lmoney, lvolume

class lday:
    @staticmethod
    def unpack(s):
        ldate, lopen, lhigh, llow, lclose, lmoney, lvolume, lbefore = struct.unpack('IIIIIfII', s)
        #print(ldate, lopen, lhigh, llow, lclose, lvolume)
        return ldate, lopen, lhigh, llow, lclose, lmoney, lvolume, lbefore

def mtest1(fpath):
    f = open(fpath, "rb")
    f.seek(-32*50, 2)
    for i in range(50):
        lc5.unpack(f.read(32))
    f.close()

def dtest1(fpath):
    f = open(fpath, "rb")
    f.seek(-32*50, 2)
    for i in range(50):
        lday.unpack(f.read(32))
    f.close()

# 读取k 日线数据， 输出长度lsize， 每个元素的值为 (Ci - Ci-1) / Ci-1  * 100.0, 跳跃skip个节点
# 其中 Ci 为第i个k线的收盘价， Ci-1 为第i-1个k线的收盘价
def dtest2(fpath, fd, lsize=30, skip=5):
    d = np.zeros(lsize)
    f = open(fpath, "rb")
    for i in range(lsize):
        a = lday.unpack(f.read(32))
        d[i] = a[4]

    while True:
        s = f.read(32*skip)
        if len(s) == 0:
            break
        n = len(s) / 32
        d1 = d.copy()
        d1 = shift(d1, -n+1, cval=0.0)
        d = shift(d, -n, cval=0.0)
        for i in range(n):
            d[-(n-i)] = lday.unpack(s[i*32:(i+1)*32])[4]
        for i in range(n-1):
            d1[-(n-1-i)] = d[-(n-i)]
        d2 = (d-d1)/d1 * 100.0
        if np.min(d2) < -10.1 or np.max(d2) > 10.1:
            print("skip 1") #, d, d1)
            continue
        #print(d)
        #print(d1)
        #print(d2)
        s = np.array_str(d2, max_line_width=1024, precision=8)
        fd.write(s[1:-1])
        fd.write('\n')

    f.close()

# 读取k 日线数据， 输出长度lsize， 每个元素的值为 (Ci - Cx) / Cx  * 100.0, 跳跃skip个节点
# 其中 Ci 为第i个k线的收盘价， Cx 为首元素前一个k线的收盘价（同一段中其为固定值）
# 后续走势格式: fname date0 c0 v0 o1 h1 l1 c1 v1 o2 h2 l2 c2 v2   (其中c0是线段最后一个节点)
def dtest3(fpath, fd, fd1, lsize=30, skip=5):
    if os.path.getsize(fpath) < 10240:
        print("%s to small, skip!")
        return 0
    d = np.zeros(lsize)
    f = open(fpath, "rb")
    for i in range(lsize):
        k = lday.unpack(f.read(32))
        d[i] = k[4]

    c = 0
    while True:
        s = f.read(32*skip)
        if len(s) == 0:
            break
        s1 = f.read(32*2)
        if len(s1) != 32*2:
            break
        n = int(len(s) / 32)  # result is float in python3
        #print(repr(d), repr(n))
        Cx = d[n-1]
        d = shift(d, -n, cval=0.0)
        k0 = 0
        for i in range(n):
            k0 = lday.unpack(s[i*32:(i+1)*32])
            d[-(n-i)] = k0[4]

        d2 = (d-Cx)/Cx * 100.0

        #s = np.array_str(d2, max_line_width=1024, precision=8)
        #fd.write(s[1:-1])
        fd.write(" ".join(["%.4f"%i for i in d2]))
        fd.write('\n')

        # 报错后续走势到另一文件中
        k1 = lday.unpack(s1[:32])
        k2 = lday.unpack(s1[32:])
        ss = "%s %d %d %d %d %d %d %d %d %d %d %d %d %d\n" %(fpath[-12:], k0[0], k0[4], k0[6],
                              k1[1], k1[2], k1[3], k1[4], k1[6],
                              k2[1], k2[2], k2[3], k2[4], k2[6])
        fd1.write(ss)
        c += 1

    f.close()
    return c

###############
################# 从 tushare 增量下载日线数据
import os
import io
import datetime
import tushare as ts
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

def ts_down_increasely(basedir='e:/stock/list11'):
    for fname in os.listdir(basedir):
        if len(fname) != 10:
            continue
        code = fname[-10: -4]
        fpath = "%s/%s" %(basedir, fname)
        l = lastline(fpath)
        start = dateadd(l[:8], 1)
        df = ts.get_k_data(code=code,autype='hfq',ktype='D',start=start)
        out = io.StringIO()
        df.to_csv(out, header=None, index=None, sep=' ')
        with open(fpath, "a") as fo:
            out.seek(0, 0)
            for line in out.readlines():
                r = line.strip().split()
                r[0] = r[0].replace('-', '')
                fo.write(" ".join(r[:-1]))
                fo.write('\n')
            out.close()




if __name__ == '__main__':
    import os
    #mtest1(r'E:\stock\new_yhzq_v21\vipdoc\sz\fzline\sz300080.lc5')

    basepath = r'e:\stock\sh\lday'
    fd = open(r"e:\stock\lines\line_sh", "w")
    fd1 = open(r"e:\stock\lines\tag_sh", "w")
    for f in os.listdir(basepath):
        fpath = r'%s\%s' % (basepath, f)
        c = dtest3(fpath, fd, fd1)
        print(fpath, c)

    fd.close()
    fd1.close()


    # fout = open(r"e:\stock\xx", "w")
    # for i in (300080, 20,21,22,23,25,26,27,28,29,30,300608,31,32):
    #     fpath = r'e:\stock\sz\lday\sz%06d.day'%i
    #     if not os.path.exists(fpath):
    #         continue
    #     print( fpath )
    #     dtest3(fpath, fout)
    # fout.close()



#encoding=utf-8

def lines_strip(f_src, f_tgt):
    fo = open(f_tgt, "w")
    for l in open(f_src):
        l = l.strip()
        a = l.split()
        fo.write(" ".join(a)); fo.write("\n")
    fo.close()

def merge():
    # 把tag文件按数字顺序合并
    fo = open("e:/stock/tag0", "w")
    for i in range(1, 126):
        for l in open("e:/stock/lines/tag%d"%i):
            fo.write(l)
    fo.close()

def split(f_tag, f_lines, outdir, max_class):
    # 聚类结果对应tag文件中的后续走势数据, 按聚类分tag文件
    # ft.split("lines/tag_sz", "lines/line_sz", "e:/stock/out_sz", 100)
    # ft.split("lines/tag_sh", "lines/line_sh", "e:/stock/out_sh", 100)
    # import os
    fs = [open("%s/%d"%(outdir, i), "w") for i in range(max_class)]
    fn = open("%s/ass"%outdir)  # 聚类结果文件， 每行的内容是类别序号， 行号则对应数据的行号
    for l in open(f_tag):
        fs[int(fn.readline().strip())].write(l)
    fn.close()
    for f in fs: f.close()

    # 拆分原始线段数据
    fs = [open("%s/l_%d" % (outdir, i), "w") for i in range(max_class)]
    fn = open("%s/ass" % outdir)  # 聚类结果文件， 每行的内容是类别序号， 行号则对应数据的行号
    for l in open(f_lines):
        fs[int(fn.readline().strip())].write(l)
    fn.close()
    for f in fs: f.close()

# 聚类结果， 统计各类中后续2日的走势情况
def stat_onefile(f):
    #print("--- %s"%f)
    count = 0
    v1, v2, v3, v4, v5 = 0, 0, 0, 0, 0  # v1: >=2
    for l in open(f):
        a = l.strip().split()
        if len(a) < 12:
            continue # 小于2个后续交易日的,忽略
        count += 1
        c0, c1, c2 = int(a[2]), int(a[7]), int(a[12])
        l1, l2 = (c1-c0)/c0 * 100.0, (c2-c1)/c1*100.0
        if l1 < 2 and l2 >=2:       v1 += 1
        if l1 >= 1 and l2 >= 2:        v2 += 1
        if l2 >= 1:   v3 += 1
        if l2 <= 0.0: v4 += 1
        if (int(a[10])-c1) / c1 * 100.0 >= 3: v5 += 1  #最高价>2%
    if count < 50: #数量小于50个的忽略掉
        return
    print("========= % 6s count: \t %d \t%.2f \t%.2f \t%.2f \t%.2f \t%.2f"%( f, count,
            v1/count*100, v2/count*100, v3/count*100, v4/count*100, v5/count*100))

def stat(fdir):
    # ft.stat('e:/stock/out_sh')
    import os
    # fname date0 c0 v0 o1 h1 l1 c1 v1 o2 h2 l2 c2 v2
    #for i in [k.strip().split()[1] for k in result.strip().split('\n')]:
    #    stat_onefile("out/%s" % i)
    for fname in os.listdir(fdir):
        if fname.isdigit():
            stat_onefile("%s/%s" % (fdir, fname))

def draw_hl(f, h_lines=(0, 7)):
    count, count7 = 0, 0
    h_a = []
    l_a = []
    for l in open(f):
        a = l.strip().split()
        if len(a) < 12:
            continue # 小于2个后续交易日的,忽略
        count += 1
        c1 = int(a[7])
        h, l = int(a[10]), int(a[11])
        if len(a) > 14:
            b = a[14:]
            for i in range(int(len(b)/5)):
                h1, l1 = int(b[i*5+1]), int(b[i*5+2])
                if h1 > h: h = h1
                if l1 < l: l = l1
        rh = (h-c1)/c1 * 100.0
        rl = (l-c1)/c1 * 100.0
        if rh >= 7:
            count7 += 1
            print("%s  %s  %.2f \t%.2f"%(a[0], a[1], rh, rl))
        h_a.append(rh)
        l_a.append(rl)

    print("=====%d    %.2f"%(count, count7/count*100.0))
    import matplotlib.pyplot as plt
    h_a.sort()
    l_a.sort()
    plt.ylim(-20, 30)
    plt.plot(h_a)
    plt.plot(l_a)
    import numpy as np
    for i in h_lines:  #(-10, -3, 0, 3, 5, 9, 15):
        plt.plot(np.ones(len(l_a))*i)
    plt.show()

def figureit(fpath, n=50, ymin=-50, ymax=100):
    # 绘制图形, 从k线生成的数据那个， 聚类后其中的一类， split_lines() 生成的文件
    # 由于数据量大， 从里面随机抽取n个绘制
    import sys, numpy as np
    import random
    import matplotlib.pyplot as plt
    dc = np.loadtxt(fpath)
    centroid = np.mean(dc, axis=0)
    rng = [i for i in range(dc.shape[0])]
    random.shuffle(rng)
    if n > dc.shape[0]:
        n = dc.shape[0]
    for v in range(n):
        #i = random.randint(0, dc.shape[0]-1)
        plt.plot(dc[rng[v], :])
    plt.plot(centroid, linewidth=5.0, linestyle='--', color='red')
    plt.ylim(ymin, ymax)
    #plt.legend()
    plt.show()


# 生成用于神经网络学习的数据
'''
1. 数据只要2010年后的
2. 数据段长度 n=60, 后续长度 m=10, 后续的数据先保存 code, date, close[-1], high, low
3. open, close, high, low, volume 分别生成一行
4. 后续走势分类, (最高价-C)/C:  <-0.3 =>1, <-0.2 =>2, <-0.1 =>3,  <-0.05 =>4, <-0.02 =>5, <0.02 =>6, <0.05 =>7, <0.1 =>8
'''
import numpy as np
import random
import time
import os
from six.moves import cPickle as pickle

# 最高价 涨幅 分类, 共 11 类
def hclass(hr):
    if hr < -.3: return 1
    elif hr < -.2: return 2
    elif hr < -.1: return 3
    elif hr < -0.05: return 4
    elif hr < -0.02: return 5
    elif hr < 0.02: return 6
    elif hr < 0.05: return 7
    elif hr < 0.1: return 8
    elif hr < 0.2: return 9
    elif hr < 0.3: return 10
    else: return 11

def load_k_data(flist, sample_count, fo, n=60, m=10, skip=3):
    begin_date = 20100101
    nrows = 5
    j = 0
    fidx = 0

    dataset = np.ndarray(shape=(sample_count, nrows, n), dtype=np.float32)
    labels = np.zeros(sample_count)

    for fpath in flist:
        fidx += 1
        code = fpath[-10:-4]
        print("code=%s %d %d"%(code, j, fidx))
        dc = np.loadtxt(fpath, dtype=float)
        for i in range(dc.shape[0]):
            if dc[i, 0] >= begin_date:
                Cx, Vx = dc[i, 2], dc[i, 5]  # 第一个交易日的收盘价 和 volume
                dc = dc[i:, :]
                break
        zsize = int((dc.shape[0]-(m+n))/skip)
        if zsize <= 0:
            continue
        #dataset = np.ndarray(shape=(zsize, nrows, n), dtype=np.float32)
        for i in range(0, zsize*skip, skip):
            v = dc[i:i+n, :]
            dataset[j, 0, :] = (v[:, 1] - Cx) / Cx  # open
            dataset[j, 1, :] = (v[:, 2] - Cx) / Cx  # close
            dataset[j, 2, :] = (v[:, 3] - Cx) / Cx  # high
            dataset[j, 3, :] = (v[:, 4] - Cx) / Cx  # low
            dataset[j, 4, :] = (v[:, 5] - Vx) / Vx  # volume
            Cx, Vx = v[skip-1, 2], v[skip-1, 5]

            # next
            nv = dc[i+n:i+n+m, :]
            C = nv[0, 2]
            labels[j] = hclass((np.max(nv[:, 3])-C)/C)
            print("%s %d %.4f %.4f"%(code, int(nv[0, 0]),  (np.max(nv[:, 3])-C)/C, (np.min(nv[:, 4])-C)/C ), file=fo)
            j += 1
            if j >= sample_count:
                return dataset, labels, fidx
    if j < sample_count:
        return dataset[:j, :, :], labels[:j], fidx
    else:
        return dataset, labels, fidx


def generate_datasets(basedir='e:/stock/list11', outdir='e:/stock/out11'):
    flist = ["%s/%s"%(basedir, fname) for fname in os.listdir(basedir)]
    random.seed(time.time())
    random.shuffle(flist)

    # 生成2个数据集
    for i in range(2):
        with open("%s/train%d.txt"%(outdir,i), "w") as fo:
            train_x, train_y, fidx = load_k_data(flist, 600000, fo)
            flist = flist[fidx:]
        with open("%s/test%d.txt"%(outdir, i), "w") as fo:
            test_x, test_y, fidx = load_k_data(flist, 100000, fo)
            flist = flist[fidx:]

        save = {'train_x': train_x,  'train_y': train_y,
                'test_x': test_x, 'test_y': test_y }
        with open("%s/datasets%d.pickle" % (outdir, i), 'wb') as f:
            pickle.dump(save, f, pickle.HIGHEST_PROTOCOL)

def load_datasets():
    from six.moves import cPickle as pickle
    import os
    os.chdir("e:/stock/out11")
    pickle_file = 'datasets.pickle'
    with open(pickle_file, 'rb') as f:
        save = pickle.load(f)
    return save

if __name__ == '__main__':
    generate_datasets()
    # import sys
    # sys.path.insert(0, 'd:/worksrc/pycode/stock'); import filetool as ft
    # import os
    # os.chdir("e:/stock")
    # split("lines/tag_sz", "lines/line_sz", "e:/stock/out_sz", 100)
    # split("lines/tag_sh", "lines/line_sh", "e:/stock/out_sh", 100)
    # stat("out_sz")
    # #stat("out_sh")

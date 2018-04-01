
#encoding=utf-8

import os
import time
import tushare as ts
import io
import datetime
from multiprocessing import Pool
import numpy as np
from six.moves import cPickle as pickle


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
def downtushare_hday(basedir = "e:/stock/list11", f_slist = 'e:/stock/stocklist.txt', process_count=8):
    import os

    try: os.mkdir(basedir)
    except: pass

    slist = [(l.split(',')[0], l.split(',')[15], basedir) for l in open(f_slist, encoding='gbk')]
    with Pool(process_count) as p:
        p.map(__one_stock_hday, slist)
    # for a in slist: __one_stock(a)

# 下载 stock list 列表
def downtushare_stocklist(f_slist="e:/stock/stocklist.txt"):
    df = ts.get_stock_basics()
    df.to_csv(f_slist, sep=',', header=None)  # mode='a', index=None
    print("down stock list done!")

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

def ts_down_increasely(basedir='e:/stock/list11', process_count=8):
    flist = ["%s/%s"%(basedir, fname) for fname in os.listdir(basedir)]
    with Pool(process_count) as p:
        p.map(__ts_down_increasely, flist)


# 切分 k 线数据为 lsize 长度的向量数据,
# 读取k 日线数据， 输出长度lsize， 每个元素的值为 (Ci - Cx) / Cx  * 100.0, 跳跃skip个数据,
#    nextdays: 后续走势取多少个
# 其中 Ci 为第i个k线的收盘价， Cx 为首元素前一个k线的收盘价（同一段中其为固定值）
# 后续走势格式: fname date0 c0 maxC minC,   其中 maxC为后续 nextdays 中的最高收盘价涨幅(相对lsize最后一个数据的收盘价)
# 文件数据格式 date open high low
def split_one_file(args): # fpath, nH=11, loop=6, ratio=1.3):
    fpath, file_pos, fd, fd1, lsize , skip, nextdays  = args
    with open(fpath) as fp:
        fp.seek(file_pos)
        dc = np.loadtxt(fp, dtype=float)

    #mindate = 20100101 # 取20100101 之后的数据
    fcode = fpath[-10:-4]

    count = 0
    for i in range(0, dc.shape[0]-lsize-nextdays, skip):
        d = dc[i: i+lsize, 2] # close
        C = d[0]
        d = (d - C) / C * 100.0

        fd.write(" ".join(["%.4f" % i for i in d]))
        fd.write('\n')

        C = dc[i+lsize-1, 2]
        d1 = dc[i+lsize:i+lsize+nextdays, 2]
        fd1.write("%s %d %.4f %.4f\n"%(fcode, dc[i+lsize, 0], (np.max(d1)-C)/C*100.0, (np.min(d1)-C)/C*100.0))
        count +=1
    print("===", fcode, count)

def split_k_data(basedir = "e:/stock/list11",
                 lines_file="e:/stock/2010_lines0330.txt",
                 tags_file="e:/stock/2010_tag0330.txt",
                 lsize=30, skip=5, nextdays=6, start_year="2010"):

    fd = open(lines_file, "w")
    fd1 = open(tags_file, "w")
    year_index_file = "%s.idx.pickle"%basedir
    if not os.path.exists(year_index_file):
        gen_index_for_k_data(basedir)
    with open(year_index_file, "rb") as fp:
        year_index = pickle.load(fp)
    arglist = [("%s/%s" % (basedir, fname), year_index.get("%s-%s"%(fname, start_year), 0), fd, fd1, lsize, skip, nextdays) for fname in os.listdir(basedir)]
    for args in arglist:
        split_one_file(args)
    fd.close()
    fd1.close()
    print("done")

    # pickle_file = "e:/stock/clusters0329.pickle"
    # lines = np.loadtxt('e:/stock/lines0329.txt', dtype='float32')
    # with open(pickle_file, "wb") as f:
    #     pickle.dump(lines, f, pickle.HIGHEST_PROTOCOL)

def load_cluster_datasets(lines_file, tags_file, cluster_result_path):
    if lines_file is None:
        lines = None
    else:
        lines = np.loadtxt(lines_file)
    tags = np.loadtxt(tags_file, dtype='float32')

    clusters = __merge_cluster_part_files(cluster_result_path)
    return lines, tags, clusters

def check_clusters(tags, clusters, lines, num_clusters = 120, winpoint = 5.0, lostpoint = 0.0, ):
    # 统计各分类的胜率
    #winpoint = 5.0 #最大收盘价涨幅点数
    #lostpoint = 0.0
    ll = []
    for i in range(num_clusters):
        c = tags[np.nonzero(clusters[:] == i)]
        if c.shape[0] == 0:
            print("cluster not found ", i)
            continue
        w = c[np.nonzero(c[:, 2] > winpoint)].shape[0]
        l = c[np.nonzero(c[:, 2] < lostpoint)].shape[0]
        ll.append((i, c.shape[0], w*100.0/c.shape[0], l*100.0/c.shape[0]))
        #print(" cluster %d  count: %d win: %.2f %%"%(i, c.shape[0], d*100.0/c.shape[0]))
    sll = sorted(ll, key=lambda i: i[2], reverse=False) #按胜率排序
    for a, b, c, d in sll:
        print(" cluster %d  count: %d win: %.2f lose %.2f" % (a, b, c, d))

    if lines is None:
        return None
    # 计算各类的中点
    centers = np.zeros((num_clusters, lines.shape[1]))
    for i in range(num_clusters):
        c = lines[np.nonzero(clusters[:] == i)]
        centers[i, :] = np.mean(c, axis=0)
    #plt.plot(cents[11, :]);  plt.show()
    return centers

# 从 check_clusters 的屏幕输出中摘取部分统计结果, 画出对应的中线
def draw_centers(centers, clusters_str):
    import matplotlib.pyplot as plt
    for line in clusters_str.strip().split("\n"):
        n = line.strip().split()  # cluster 77  count: 1334 win: 66.04 lose 18.52
        if len(n) < 8: continue
        c = int(n[1])
        plt.plot(centers[c], label=line)
    plt.legend()
    plt.show()

# 从指定类别中, 找出对应的code 和 日期, 便于画出k线
def find_cluster(centers, cluster, clusters, tags):
    tag = tags[np.nonzero(clusters[:]==cluster)]
    center = centers[cluster]
    tag = [ ("%06d"%tag[i, 0], int(tag[i, 1])) for i in range(tag.shape[0])]
    return tag, center




####################################
#### 日常的处理过程
####################################

def daily_split_one_file(args): # fpath, nH=11, loop=6, ratio=1.3):
    fpath, file_pos, fd, fd1, lsize , skip  = args
    if file_pos < 0:
        print("%s pos %d"%(fpath, file_pos))
        return
    with open(fpath) as fp:
        fp.seek(file_pos)
        dc = np.loadtxt(fp, dtype=float)
    fcode = fpath[-10:-4]
    count = 0
    for i in range(0, dc.shape[0]-lsize, skip):
        d = dc[i: i+lsize, 2] # close
        C = d[0]
        d = (d - C) / C * 100.0

        fd.write(" ".join(["%.4f" % i for i in d]))
        fd.write('\n')
        fd1.write("%s %d\n" % (fcode, dc[i + lsize, 0]) )

        count +=1
    print("===", fcode, count)

def daily_split_k_data(basedir = "e:/stock/list11",
                 lines_file="e:/stock/day_lines0330.txt",
                 tags_file="e:/stock/day_tags0330.txt",
                 start_year="2018",
                 lsize=30, skip=2):
    fd = open(lines_file, "w")
    fd1 = open(tags_file, "w")
    year_index_file = "%s.idx.pickle"%basedir
    if not os.path.exists(year_index_file):
        gen_index_for_k_data(basedir)
    with open(year_index_file, "rb") as fp:
        year_index = pickle.load(fp)
    arglist = [("%s/%s" % (basedir, fname), year_index.get("%s-%s"%(fname, start_year), -1), fd, fd1, lsize, skip) for fname in os.listdir(basedir)]
    for args in arglist:
        daily_split_one_file(args)  # if args[0].find("000035") > 0:
    fd.close()
    fd1.close()
    print("done")

# 为k线数据生成年份索引
def gen_index_for_k_data(basedir="e:/stock/list11"):
    idx = {}
    for fname in os.listdir(basedir):
        fpath = "%s/%s"%(basedir, fname)
        pos = -1
        with open(fpath, "r") as fp:
            year = "0"
            while True:
                line = fp.readline()
                if line == '': break
                y = line[:4]
                if y != year:
                    pos = fp.tell()-len(line)-2
                    if pos < 0: pos = 0
                    idx["%s-%s"%(fname, y)] = pos
                    year = y
    out_index = "%s.idx.pickle"%basedir
    with open(out_index, "wb") as fp:
        pickle.dump(idx, fp, pickle.HIGHEST_PROTOCOL)
    print("generate year index of k_data done!")

# 合并 spark 聚类输出文件
def __merge_cluster_part_files(cluster_result_path):
    cluster_file = "%s/clusters" % cluster_result_path
    if os.path.exists(cluster_file):
        clusters = np.loadtxt(cluster_file, dtype='int')  # 这个文件是spark 聚类计算结果
        return clusters
    fn = [fname for fname in os.listdir(cluster_result_path) if fname.startswith('part-')]
    fn = sorted(fn)  # spark输出的文件是多个， 需要排序后合并, 每一行是lines对应行的类别编号

    with open(cluster_file, "w") as fo:
        for fname in fn:
            with open("%s/%s" % (cluster_result_path, fname)) as fin:
                for line in fin: fo.write(line)
    clusters = np.loadtxt(cluster_file, dtype='int')  # 这个文件是spark 聚类计算结果
    return clusters

# 输出对应类别的股票代码及选中日期
def daily_check_clusters(tags_file, cluster_result_path, clusters_want):
    tag = np.loadtxt(tags_file, dtype='float32')
    clusters = __merge_cluster_part_files(cluster_result_path)

    for cluster in clusters_want:
        tt = tag[np.nonzero(clusters[:] == cluster)]
        for i in range(tt.shape[0]):
            t = tt[i, :]
            print("cluster %d  code: %06d  date: %d"%(cluster, t[0], t[1]))

# 选中类别的中线绘制
def daily_draw(lines_file, cluster_result_path, info_str):
    import matplotlib.pyplot as plt
    clusters = __merge_cluster_part_files(cluster_result_path)
    lines = np.loadtxt(lines_file, dtype=float)
    for l in info_str.strip().split("\n"): # cluster 96  count: 2501 win: 61.66 lose 13.39
        n = l.strip().split()
        if len(n) < 8: continue
        cluster = int(n[1])
        dc = lines[np.nonzero(clusters[:] == cluster)]
        center = np.mean(dc, axis=0)
        plt.plot(center, label=l)
    plt.legend()
    plt.show()


if __name__ == '__main__':
    #downtushare_60()
    #format_files_60()
    ts_down_increasely()
    pass

'''
bin\spark-shell --driver-memory=4g


import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

val model_path="e:/stock/kmeans_spark_model"
val lines_path="e:/stock/lines0330.txt"
val output_path="e:/stock/clustering_out"

val numClusters = 60
val numIterations = 100

// Load and parse the data
val data = sc.textFile(lines_path)
val parsedData = data.map(s => Vectors.dense(s.split(" ").map(_.toDouble)))
parsedData.cache
parsedData.first
parsedData.count

val clusters = KMeans.train(parsedData, numClusters, numIterations)

// test different num_clusters and compare cost
//val ks:Array[Int] = Array(10,15, 20, 25, 30, 50)
//val ks:Array[Int] = Array(60,70,100,150)

//ks.foreach(cluster => {
// val model:KMeansModel = KMeans.train(parsedData, cluster, numIterations)
// val ssd = model.computeCost(parsedData)
// println("sum of squared distances of points to their nearest center when k=" + cluster + " -> "+ ssd)
//})

clusters.save(sc, model_path)
val sameModel = KMeansModel.load(sc, model_path)

// here is what I added to predict data points that are within the clusters
sameModel.predict(parsedData).foreach(println)

val out = clusters.predict(parsedData)
out.saveAsTextFile(output_path)
'''
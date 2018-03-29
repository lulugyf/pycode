#coding=utf-8

import numpy as np
import numpy
import theano
import theano.tensor as T
from theano import function, config, shared, sandbox

from sklearn import cluster, datasets
import matplotlib.pyplot as plt
from six.moves import cPickle as pickle
import os

# https://github.com/erogol/KLP_KMEANS/blob/master/klp_kmeans.py
# https://gist.github.com/erogol/7946246   ===
def rsom(data, cluster_num, alpha, epochs=-1, batch=1, verbose=False):
    rng = np.random

    # From Kohonen's paper
    if epochs == -1:
        print( data.shape[0] )
        epochs = 500 * data.shape[0]

    # Symmbol variables
    X = T.dmatrix('X')
    WIN = T.dmatrix('WIN')

    # Init weights random
    W = theano.shared(rng.randn(cluster_num, data.shape[1]), name="W")
    W_old = W.get_value()

    # Find winner unit
    bmu = ((W ** 2).sum(axis=1, keepdims=True) + (X ** 2).sum(axis=1, keepdims=True).T - 2 * T.dot(W, X.T)).argmin(
        axis=0)
    dist = T.dot(WIN.T, X) - WIN.sum(0)[:, None] * W
    err = abs(dist).sum()

    update = function([X, WIN], outputs=err, updates=[(W, W + alpha * dist)], mode="FAST_RUN")
    find_bmu = function([X], bmu, mode="FAST_RUN")

    if any([x.op.__class__.__name__ in ['Gemv', 'CGemv', 'Gemm', 'CGemm'] for x in
            update.maker.fgraph.toposort()]):
        print('Used the cpu')
    elif any([x.op.__class__.__name__ in ['GpuGemm', 'GpuGemv'] for x in
              update.maker.fgraph.toposort()]):
        print('Used the gpu')
    else:
        print('ERROR, not able to tell if theano used the cpu or the gpu')
        print(update.maker.fgraph.toposort())

    # Update
    for epoch in range(epochs):
        C = 0
        for i in range(0, data.shape[0], batch):
            if i + batch >= data.shape[0]:
                i = data.shape[0]-batch
            D = find_bmu(data[i:i + batch, :])
            S = np.zeros([batch, cluster_num])
            S[range(batch), D] = 1
            cost = update(data[i:i + batch, :], S)

        if epoch % 10 == 0 and verbose:
            print("Avg. centroid distance -- ", cost.sum(), "\t EPOCH : ", epoch)
    return W.get_value()

def kmeans(X, cluster_num, numepochs, learningrate=0.01, batchsize=100, verbose=True):
    rng = numpy.random
    W = rng.randn(cluster_num, X.shape[1])
    X2 = (X ** 2).sum(1)[:, None]
    for epoch in range(numepochs):
        for i in range(0, X.shape[0], batchsize):
            D = -2 * numpy.dot(W, X[i:i + batchsize, :].T) + (W ** 2).sum(1)[:, None] + X2[i:i + batchsize].T
            S = (D == D.min(0)[None, :]).astype("float").T
            W += learningrate * (numpy.dot(S.T, X[i:i + batchsize, :]) - S.sum(0)[:, None] * W)
        if verbose and epoch % 10 == 0:
            print("epoch", epoch, "of", numepochs, " cost: ", D.min(0).sum())
    return W


def mytest():
    # Test Codes
    blobs = datasets.make_blobs(n_samples=4000, random_state=8)
    noisy_moons = datasets.make_moons(n_samples=4000, noise=.05)
    noisy_circles = datasets.make_circles(n_samples=2000, factor=.5,
                                          noise=.05)

    DATA = blobs[0]

    import time

    t1 = time.time()
    W = rsom(DATA, 3, alpha=0.001, epochs=100, batch=100, verbose=True)
    t2 = time.time()

    t3 = time.time()
    W2 = kmeans(DATA, 3, numepochs=100, batchsize=10, learningrate=0.001)
    t4 = time.time()

    print("RSOM : ", t2 - t1 )
    print("kmeans : ", t4 - t3 )

    plt.scatter(DATA[:, 0], DATA[:, 1], color='red')
    plt.scatter(W[:, 0], W[:, 1], color='blue', s=20, edgecolor='none')
    plt.scatter(W2[:, 0], W2[:, 1], color='green', s=20, edgecolor='none')
    plt.show()



# 切分 k 线数据为 lsize 长度的向量数据,
# 读取k 日线数据， 输出长度lsize， 每个元素的值为 (Ci - Cx) / Cx  * 100.0, 跳跃skip个数据,
#    nextdays: 后续走势取多少个
# 其中 Ci 为第i个k线的收盘价， Cx 为首元素前一个k线的收盘价（同一段中其为固定值）
# 后续走势格式: fname date0 c0 maxC minC,   其中 maxC为后续 nextdays 中的最高收盘价涨幅(相对lsize最后一个数据的收盘价)
# 文件数据格式 date open high low
def split_one_file(args): # fpath, nH=11, loop=6, ratio=1.3):
    fpath, fd, fd1, lsize , skip, nextdays  = args
    dc = np.loadtxt(fpath, dtype=float)
    if dc.shape[0] < 100: #太短的忽略
        return 0
    mindate = 20100101 # 取20100101 之后的数据
    fcode = fpath[-10:-4]

    print("   ", dc[0, 0], dc[-1, 0], fpath)
    for i in range(dc.shape[0]):
        if dc[i, 0] > mindate:
            dc = dc[i:, :]
            break
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

def split_k_data():
    basedir = "e:/stock/list11"
    fd = open("e:/stock/lines0329.txt", "w")
    fd1 = open("e:/stock/tag0329.txt", "w")
    arglist = [("%s/%s" % (basedir, fname), fd, fd1, 30, 5, 6) for fname in os.listdir(basedir)]
    for args in arglist:
        if True or args[0].find('603997') > 0:
            split_one_file(args)
    fd.close()
    fd1.close()

    pickle_file = "e:/stock/clusters0329.pickle"
    lines = np.loadtxt('e:/stock/lines0329.txt', dtype='float32')
    with open(pickle_file, "wb") as f:
        pickle.dump(lines, f, pickle.HIGHEST_PROTOCOL)


def stock_data():
    from six.moves import cPickle as pickle
    pickle_file = "e:/stock/clusters0329.pickle"
    with open(pickle_file, "rb") as f: lines = pickle.load(f)
    #DATA = np.loadtxt('e:/stock/lines0329.txt', dtype='float32')

    W = rsom(lines, 30, alpha=0.001, epochs=100, batch=200, verbose=True)

    tag = np.loadtxt('e:/stock/tag0329.txt', dtype='float32')
    clusters = np.loadtxt('e:/stock/out/out/clusters', dtype='int') # 这个文件是spark 聚类计算结果
    if os.path.exists(pickle_file):
        with open(pickle_file, 'rb') as f:
            lines = pickle.load(f)
    else:
        lines = np.loadtxt('e:/stock/lines0329.txt', dtype='float32')
        with open(pickle_file, "wb") as f:
            pickle.dump(lines, f, pickle.HIGHEST_PROTOCOL)
    return tag, lines, clusters

def succ(tag, clusters, lines):
    # 计算各分类的胜率
    winpoint = 5.0 #最大收盘价涨幅点数
    lostpoint = 0.0
    num_clusters = 60
    ll = []
    for i in range(num_clusters):
        c = tag[np.nonzero(clusters[:] == i)]
        w = c[np.nonzero(c[:, 2] > winpoint)].shape[0]
        l = c[np.nonzero(c[:, 2] < lostpoint)].shape[0]
        ll.append((i, c.shape[0], w*100.0/c.shape[0], l*100.0/c.shape[0]))
        #print(" cluster %d  count: %d win: %.2f %%"%(i, c.shape[0], d*100.0/c.shape[0]))
    sll = sorted(ll, key=lambda i: i[2], reverse=False) #按胜率排序
    for a, b, c, d in sll:
        print(" cluster %d  count: %d win: %.2f lose %.2f" % (a, b, c, d))

    # 计算各类的中点
    cents = np.zeros((num_clusters, lines.shape[1]))
    for i in range(num_clusters):
        c = lines[np.nonzero(clusters[:] == i)]
        cents[i, :] = np.mean(c, axis=0)
    #plt.plot(cents[11, :]);  plt.show()
    return cents

'''
bin\spark-shell --driver-memory=4g


import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

// Load and parse the data
val data = sc.textFile("e:/stock/lines0329.txt")
val parsedData = data.map(s => Vectors.dense(s.split(" ").map(_.toDouble)))
parsedData.cache
parsedData.first
parsedData.count

// Cluster the data into two classes using KMeans
val numClusters = 60
val numIterations = 100
val clusters = KMeans.train(parsedData, numClusters, numIterations)

val ks:Array[Int] = Array(10,15, 20, 25, 30, 50)
val ks:Array[Int] = Array(60,70,100,150)

ks.foreach(cluster => {
 val model:KMeansModel = KMeans.train(parsedData, cluster, numIterations)
 val ssd = model.computeCost(parsedData)
 println("sum of squared distances of points to their nearest center when k=" + cluster + " -> "+ ssd)
})

clusters.save(sc, "./x")
val sameModel = KMeansModel.load(sc, "./x")

// here is what I added to predict data points that are within the clusters
clusters.predict(parsedData).foreach(println)

val out = clusters.predict(parsedData)
out.saveAsTextFile("./out")

'''
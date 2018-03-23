from numpy import *
import numpy as np
#encoding=utf-8

# set path=d:\dev\Anaconda3\Scripts;d:\dev\Anaconda3;C:\WINDOWS\system32;C:\WINDOWS;D:\dev\git\usr\bin
# set QT_PLUGIN_PATH=d:\dev\Anaconda3\Library\plugins  matplotlib failed, need this
'''
set path=d:\dev\Anaconda3\Scripts;d:\dev\Anaconda3;C:\WINDOWS\system32;C:\WINDOWS
set QT_PLUGIN_PATH=d:\dev\Anaconda3\Library\plugins

python

import sys, numpy as np
#import matplotlib.pyplot as plt
from importlib import reload
sys.path.insert(0, 'e:/worksrc/pycode/stock')
import kmeans_np as k
import lc5 as l
import filetool as ft

'''

def loadDataSet(fileName):  # general function to parse tab -delimited floats
    dataMat = []  # assume last column is target value
    fr = open(fileName)
    for line in fr.readlines():
        curLine = line.strip().split('\t')
        fltLine = list(map(float, curLine))  # map all elements to float()
        dataMat.append(fltLine)
    return dataMat


def distEclud(vecA, vecB):
    return sqrt(sum(power(vecA - vecB, 2)))  # la.norm(vecA-vecB)


def randCent(dataSet, k):
    n = shape(dataSet)[1]
    centroids = mat(zeros((k, n)))  # create centroid mat
    for j in range(n):  # create random cluster centers, within bounds of each dimension
        minJ = min(dataSet[:, j])
        rangeJ = float(max(dataSet[:, j]) - minJ)
        centroids[:, j] = mat(minJ + rangeJ * random.rand(k, 1))
    return centroids


def kMeans(dataSet, k, distMeas=distEclud, createCent=randCent):
    m = shape(dataSet)[0]
    if m == 0:
        return None, None
    clusterAssment = mat(zeros((m, 2)))  # create mat to assign data points
    # to a centroid, also holds SE of each point
    centroids = createCent(dataSet, k)
    clusterChanged = True
    while clusterChanged:
        clusterChanged = False
        for i in range(m):  # for each data point assign it to the closest centroid
            minDist = inf
            minIndex = -1
            for j in range(k):
                distJI = distMeas(centroids[j, :], dataSet[i, :])
                if distJI < minDist:
                    minDist = distJI
                    minIndex = j
            if clusterAssment[i, 0] != minIndex: clusterChanged = True
            clusterAssment[i, :] = minIndex, minDist ** 2
        #print(centroids)
        for cent in range(k):  # recalculate centroids
            ptsInClust = dataSet[nonzero(clusterAssment[:, 0].A == cent)[0]]  # get all the point in this cluster
            centroids[cent, :] = mean(ptsInClust, axis=0)  # assign centroid to mean
    return centroids, clusterAssment


def biKmeans(dataSet, k, distMeas=distEclud):
    m = shape(dataSet)[0]
    clusterAssment = mat(zeros((m, 2)))
    centroid0 = mean(dataSet, axis=0).tolist()[0]
    centList = [centroid0]  # create a list with one centroid
    for j in range(m):  # calc initial Error
        clusterAssment[j, 1] = distMeas(mat(centroid0), dataSet[j, :]) ** 2
    while (len(centList) < k):
        lowestSSE = inf
        for i in range(len(centList)):
            ptsInCurrCluster = dataSet[nonzero(clusterAssment[:, 0].A == i)[0],
                               :]  # get the data points currently in cluster i
            centroidMat, splitClustAss = kMeans(ptsInCurrCluster, 2, distMeas)
            if centroidMat is None:
                continue
            sseSplit = sum(splitClustAss[:, 1])  # compare the SSE to the currrent minimum
            sseNotSplit = sum(clusterAssment[nonzero(clusterAssment[:, 0].A != i)[0], 1])
            print("sseSplit, and notSplit: ", sseSplit, sseNotSplit)
            if (sseSplit + sseNotSplit) < lowestSSE:
                bestCentToSplit = i
                bestNewCents = centroidMat
                bestClustAss = splitClustAss.copy()
                lowestSSE = sseSplit + sseNotSplit
        bestClustAss[nonzero(bestClustAss[:, 0].A == 1)[0], 0] = len(centList)  # change 1 to 3,4, or whatever
        bestClustAss[nonzero(bestClustAss[:, 0].A == 0)[0], 0] = bestCentToSplit
        print('the bestCentToSplit is: ', bestCentToSplit, len(centList))
        print('the len of bestClustAss is: ', len(bestClustAss))
        centList[bestCentToSplit] = bestNewCents[0, :].tolist()[0]  # replace a centroid with two best centroids
        centList.append(bestNewCents[1, :].tolist()[0])
        clusterAssment[nonzero(clusterAssment[:, 0].A == bestCentToSplit)[0], :] = bestClustAss  # reassign new clusters, and SSE
    return mat(centList), clusterAssment



# r'H:\Downloads\devbooks\machinelearninginaction\Ch10\testset.txt'
# r'H:\Downloads\devbooks\machinelearninginaction\Ch10\testset2.txt'
def t0(fpath, clusters_num):
    import matplotlib.pyplot as plt

    d2 = np.loadtxt(fpath, dtype=float)
    c, a = kMeans(d2, clusters_num)
    return c, a
    # for i in range(clusters_num):
    #     a0 = d2[np.nonzero(a[:, 0].A == i)[0]]
    #     plt.scatter(a0[:, 0], a0[:, 1], marker=markers[i%len(markers)])
    # plt.scatter(c[:, 0].A, c[:, 1].A, color='red', marker="+")
    # plt.show()

def t1(n):
    clusters_num = n
    dd = np.loadtxt('e:/stock/xx', dtype=float)
    c, a = biKmeans(dd, clusters_num)
    print(np.sum(a[:, 1]))
    np.savetxt(r"e:\stock\a", a, fmt="%d")
    np.savetxt(r"e:\stock\c", c, fmt="%.4f")
    return c, a


def t2():
    c0 = np.loadtxt('e:/stock/c1', dtype=float).reshape(50, 30)
    c1 = np.loadtxt('e:/stock/c2', dtype=float).reshape(50, 30)
    for i in range(c0.shape[0]):
        dmn = np.inf
        dmx = 0
        idx = 0
        for j in range(c1.shape[0]):
            d0 = distEclud(c0[i, :], c1[j, :])
            if d0 < dmn: dmn = d0; idx = j
            if d0 > dmx: dmx = d0
        print(idx, int(dmn), int(dmx))
def t3():
    f = open('xx1', 'w')
    h = ["F%d"%i for i in range(30)]
    h.append("Name")
    f.write(",".join(h)); f.write('\n')
    f1 = open('xx')
    f2 = open('a')   # np.savetxt(r"e:\stock\a", a, fmt="%d")
    while True:
        l = f1.readline().strip()
        if l == '': break
        x = l.split()
        cn = f2.readline().split()[0]
        # if cn not in ( "6",): #("0", "2", "4", "5", "6", "9"):
        #     continue
        x.append(cn)
        f.write(",".join(x))
        f.write('\n')
    f.close()
    f1.close()
    f2.close()

def t4(tp='r'):
    # 可视化  conda install pandas  多维数据 可视化
    # http://cloga.info/%E6%95%B0%E6%8D%AE%E5%88%86%E6%9E%90/2016/10/12/multivariate-data-visualization
    import pandas as pd
    import matplotlib.pyplot as plt
    data = pd.read_csv('file:///e:/stock/xx1')
    from pandas.tools.plotting import andrews_curves
    from pandas.tools.plotting import parallel_coordinates
    from pandas.tools.plotting import radviz

    plt.figure()
    if tp == 'r':
        radviz(data, 'Name')
    elif tp == 'a':
        andrews_curves(data, 'Name')
    elif tp == 'p':
        parallel_coordinates(data, 'Name')
    plt.show()

markers = (
    ".", ",", "o", "v", "^", "<", ">", "1", "2", "3", "4", "8", "s", "p", "P", "*", "h", "H", "+", "x", "X", "D", "d",
    "|", "_")
def show_scatter():
    # 打印 spark 计算出来的聚类结果
    import matplotlib.pyplot as plt
    d1 = np.loadtxt(r'H:\Downloads\a.txt', dtype=float)
    a = np.loadtxt(r'H:\Downloads\out', dtype=int)
    for i in range(4):
        a0 = d1[np.nonzero(a[:] == i)]
        plt.scatter(a0[:, 0], a0[:, 1], marker=markers[i])
    plt.show()
    #plt.scatter(c[:, 0].A, c[:, 1].A, color='red', marker="D")


def ttttttx_main():
    import sys, numpy as np
    import matplotlib.pyplot as plt
    from importlib import reload
    sys.path.insert(0, 'e:/worksrc/pycode/stock')
    import kmeans_np as k

    reload(k)
    k.t0(r'H:\Downloads\devbooks\machinelearninginaction\Ch10\testset.txt', 4)
    k.t0(r'G:\stock\machinelearninginaction\Ch10\testset.txt', 4)
    k.t0(r'H:\Downloads\devbooks\machinelearninginaction\Ch10\testset2.txt', 3)

    markers = (".",",","o","v","^","<",">","1","2","3","4","8","s","p","P","*","h","H","+","x","X","D","d","|","_")

    d1 = np.loadtxt(r'H:\Downloads\devbooks\machinelearninginaction\Ch10\testset.txt', dtype=float)
    d2 = np.loadtxt(r'H:\Downloads\devbooks\machinelearninginaction\Ch10\testset2.txt', dtype=float)
    plt.scatter(d1[:, 0], d1[:, 1])
    plt.show()

    c, a = k.kMeans(d1, 4)
    for i in range(4):
        a0 = d2[np.nonzero(a[:, 0].A == i)[0]]
        plt.scatter(a0[:, 0], a0[:, 1], marker=markers[i])
    plt.scatter(c[:, 0].A, c[:, 1].A, color='red', marker="D")



    dd = np.loadtxt('e:/stock/xx', dtype=float)
    centroids, assignments = k.kMeans(dd, 20)

    dc = np.loadtxt("c")

    for i in range(dc.shape[0]): plt.plot(dc[i, :], label="line %d"%i)
    plt.ylim(-50, 100)
    plt.legend()
    plt.show()

def figureit(fpath):
    # 绘制图形
    import sys, numpy as np
    import matplotlib.pyplot as plt
    dc = np.loadtxt(fpath)
    for i in range(dc.shape[0]): plt.plot(dc[i, :], label="line %d" % i)
    plt.ylim(-50, 100)
    plt.legend()
    plt.show()

def bk(basepath, idx, clusters_num=20):
    import os
    if os.path.exists("%s/cents_%d"%(basepath, idx)):
        return
    if os.path.getsize('%s/line%d'%(basepath, idx)) == 0:
        return
    dd = np.loadtxt('%s/line%d'%(basepath, idx), dtype=float)
    if dd.shape[0] == 0:
        return
    c, a = biKmeans(dd, clusters_num)
    print(np.sum(a[:, 1]))
    np.savetxt('%s/assign_%d'%(basepath, idx), a, fmt="%d")
    np.savetxt('%s/cents_%d'%(basepath, idx), c, fmt="%.4f")
    return c, a

'''
scala code in spark that used to do kmeans clustersing

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

// Load and parse the data
val data = sc.textFile("e:/stock/lines/line_sh")
val parsedData = data.map(s => Vectors.dense(s.split(" ").map(_.toDouble)))
parsedData.cache
parsedData.first
parsedData.count

// Cluster the data into two classes using KMeans
val numClusters = 60
val numIterations = 100
val clusters = KMeans.train(parsedData, numClusters, numIterations)

val ks:Array[Int] = Array(10, 15, 20, 25, 30, 50)
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
out.saveAsTextFile("e:/stock/out_sh")

'''

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        n = int(sys.argv[1])
        for i in range(1, 126):
            if i % 4 != n:
                continue
            try:
                bk('e:/stock/lines', i)
            except:
                continue
